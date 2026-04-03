[news_core.py](https://github.com/user-attachments/files/26456417/news_core.py)
"""
通用新闻稿抓取工具 v5.0 超级优化版
集成 Scrapling 核心技术：
  - curl_cffi   : 真实 TLS 指纹，绕过反爬检测
  - patchright  : 修补版 Playwright，绕过浏览器自动化检测
  - browserforge: 随机真实请求头，降低被封概率

借鉴 scrapy（下载器 Slot / Retry 中间件思路，见 scrapy.core.downloader / downloadermiddlewares.retry）：
  - 按域名礼貌限速：DOWNLOAD_DELAY + RANDOMIZE_DOWNLOAD_DELAY（0.5~1.5 倍抖动）
  - 可配置 RETRY_HTTP_CODES + 指数退避，减少 5xx/429 类瞬时失败
"""

import asyncio, os, re, sys, json, time
import logging
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from bs4 import BeautifulSoup

# 保证与 news_core.py 同目录的模块可被导入（无论从哪个 cwd 启动）
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

try:
    from scraper_settings import build_scrape_config
except Exception:
    build_scrape_config = None  # type: ignore

# ── ScrapingBee 配置 ──────────────────────────────────────
try:
    from scraper_settings import SCRAPINGBEE_API_KEY
    SCRAPINGBEE_KEY = SCRAPINGBEE_API_KEY
except Exception:
    SCRAPINGBEE_KEY = ""
# ScrapingBee 已停用，CF 域名全部走 patchright headless=False。
# CF_TURNSTILE_DOMAINS 有意设为空列表：
#   - 非空时，fetch_article_body 会对匹配域名走 ScrapingBee/搜索引擎摘要快速通道；
#   - 当前 ScrapingBee 停用后，该通道会降级为 DuckDuckGo/Bing 摘要，
#     正文质量远不如 patchright 直接渲染，故统一走 patchright。
#   - 若将来重新启用 ScrapingBee，可恢复：
#     CF_TURNSTILE_DOMAINS = ["mckinsey.com", "oliverwyman.com", "bain.com", "kearney.com"]
# 注意：不从 scraper_settings 读取，避免旧配置覆盖此处意图。
CF_TURNSTILE_DOMAINS = []

_log = logging.getLogger("news_scraper")


def _setup_logging_from_env() -> None:
    """NEWS_SCRAPER_LOG=DEBUG|INFO|WARNING 时启用标准库 logging（便于排障与对接日志收集）。"""
    raw = (os.environ.get("NEWS_SCRAPER_LOG") or "").strip().upper()
    if not raw:
        return
    if raw in ("1", "TRUE", "YES"):
        raw = "DEBUG"
    level = getattr(logging, raw, None)
    if level is None:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
# ── 可选：更好看的日志/进度条（来自 awesome-python 清单推荐的 rich 思路） ──
try:
    from rich.console import Console
    _RICH_AVAILABLE = True
    _console = Console()
except Exception:
    _RICH_AVAILABLE = False
    _console = None

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable, Image as RLImage

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font as XLFont, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except Exception:
    Workbook = None
    XLFont = PatternFill = Alignment = get_column_letter = None
    OPENPYXL_AVAILABLE = False

# ── 智能导入：优先用高级库，自动降级 ────────────────────
try:
    from curl_cffi import requests as _req
    CURL_AVAILABLE = True
    print("  ✓ curl_cffi 可用（TLS 指纹伪装）")
except ImportError:
    import requests as _req
    CURL_AVAILABLE = False
    print("  ⚠ 使用普通 requests（建议: pip install curl_cffi）")

try:
    from patchright.async_api import async_playwright
    PATCHRIGHT_AVAILABLE = True
    print("  ✓ patchright 可用（浏览器指纹修补）")
except ImportError:
    from playwright.async_api import async_playwright
    PATCHRIGHT_AVAILABLE = False
    print("  ⚠ 使用普通 playwright（建议: pip install patchright）")

# ScrapingBee 状态提示（暂停使用，改用通用 Playwright）
if SCRAPINGBEE_KEY:
    print(f"  ✓ ScrapingBee 已配置（CF Turnstile 域名将用真实截图）")

try:
    from browserforge.headers import Browser, HeaderGenerator
    BROWSERFORGE_AVAILABLE = True
    print("  ✓ browserforge 可用（随机真实请求头）")
except ImportError:
    BROWSERFORGE_AVAILABLE = False
    print("  ⚠ 使用固定请求头（建议: pip install browserforge）")

_setup_logging_from_env()

# ── 参数 ──────────────────────────────────────────────
# MONTHS=0 表示单篇模式（由 shell 脚本传入）
if len(sys.argv) >= 5:
    LIST_URL = sys.argv[1]
    _months_arg = sys.argv[2]
    FMT = sys.argv[3]
    OUTBASE = sys.argv[4]
    # 支持两种格式：月份数（如 "6"）或具体日期（如 "2025-09-01"）
    if re.match(r"^\d{4}-\d{2}-\d{2}$", _months_arg):
        _start_date = datetime.strptime(_months_arg, "%Y-%m-%d")
        _delta = datetime.now() - _start_date
        MONTHS = max(1, int(_delta.days / 30))
        _CUTOFF_OVERRIDE = _start_date  # 精确使用输入日期作为 cutoff
    else:
        MONTHS = int(_months_arg)
        _CUTOFF_OVERRIDE = None
else:
    LIST_URL = ""
    MONTHS = 6
    FMT = "3"
    OUTBASE = os.path.join(os.path.expanduser("~"), "Desktop", "news_debug")
    _CUTOFF_OVERRIDE = None

# MONTHS=0 → 单篇模式，但需要排除已知的列表页路径
_LIST_PATH_PATTERNS = [
    "/our-insights", "/our-thinking", "/featured-insights",
    "/our-work", "/insights", "/publications", "/research",
    "/news", "/press", "/media", "/blog", "/articles",
    "/market-insights", "/press-releases",
]
# 检测 URL 末段是否是已知列表页关键词（去掉尾斜杠再比较）
_url_last_seg = LIST_URL.rstrip("/").split("/")[-1].lower()
_LIST_LAST_SEGS = {"insights", "our-insights", "our-thinking", "featured-insights",
                   "our-work", "publications", "research", "news", "press", "media",
                   "blog", "articles", "market-insights", "press-releases",
                   "releases", "briefings", "perspectives", "reports"}
_is_known_list = (_url_last_seg in _LIST_LAST_SEGS or
                  any(LIST_URL.lower().rstrip("/").endswith(p.rstrip("/"))
                      for p in _LIST_PATH_PATTERNS))
SINGLE_ARTICLE_MODE = (MONTHS == 0) and not _is_known_list

SCREENSHOT_DIR = os.path.join(OUTBASE, "screenshots")
PDF_DIR        = os.path.join(OUTBASE, "pdfs")
if SINGLE_ARTICLE_MODE:
    CUTOFF = datetime(2000, 1, 1)
elif _CUTOFF_OVERRIDE is not None:
    CUTOFF = _CUTOFF_OVERRIDE  # 用户输入了精确日期
else:
    CUTOFF = datetime.now() - timedelta(days=MONTHS * 30)

os.makedirs(OUTBASE,        exist_ok=True)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)
os.makedirs(PDF_DIR,        exist_ok=True)

# ── URL 重定向表 ──────────────────────────────────────
URL_REDIRECT = {
    "schaeffler.com":          "https://www.schaeffler.com/en/media/press-releases/",
    "rolandberger.com":        "https://rolandberger-com.mynewsdesk.com/pressreleases",
    "siemens.com":             "https://press.siemens.com/global/en/pressrelease",
    "bosch.com":               "https://www.bosch-presse.de/pressportal/de/en/",
    "bmwgroup.com":            "https://www.press.bmwgroup.com/global/article/list",
    "mckinsey.com":            "https://www.mckinsey.com/about-us/new-at-mckinsey-blog",
    "bcg.com":                 "https://www.bcg.com/publications",
    "bain.com":                "https://www.bain.com/about/media-center/press-releases/",
    "deloitte.com":            "https://www2.deloitte.com/global/en/pages/about-deloitte/articles/press-releases.html",
    "kpmg.com":                "https://kpmg.com/xx/en/home/media/press-releases.html",
    "pwc.com":                 "https://www.pwc.com/gx/en/news-room/press-releases.html",
    "accenture.com":           "https://newsroom.accenture.com/news/",
    "volkswagen-newsroom.com": "https://www.volkswagen-newsroom.com/en/press-releases-3",
    "porsche-consulting.com":  "https://newsroom.porsche.com/en/company/porsche-consulting.html",
    "oliverwyman.com":         "https://www.oliverwyman.com/our-expertise/insights.html",
    "kearney.com":             "https://www.kearney.com/insights",
    "woodmac.com":              "https://www.woodmac.com/market-insights/",
}

original_url = LIST_URL
try:
    _pu = urlparse(LIST_URL)
    _netloc = (_pu.netloc or "").lower()
    _path = (_pu.path or "/").strip()
except Exception:
    _netloc, _path = "", "/"

# 只在用户输入“域名根/很泛入口”时才重定向；
# 如果用户输入的是具体列表页（例如 kearney 的 /-/categories/...），必须保留原 URL，否则会误跳到 /insights 导航页导致日期与命名全错。
for dk, rv in URL_REDIRECT.items():
    if dk in _netloc and (_path in ("", "/")):
        print(f"  → 重定向: {dk} → {rv}")
        LIST_URL = rv
        break

BASE_DOMAIN = urlparse(LIST_URL).scheme + "://" + urlparse(LIST_URL).netloc

# ════════════════════════════════════════════════════
# Scrapy 风格的抓取策略（不依赖 scrapy 包，仅复用工程实践）
# ════════════════════════════════════════════════════
# 默认值见 scraper_settings.py，可用环境变量 NEWS_SCRAPER_* 覆盖
if build_scrape_config is not None:
    SCRAPE_CONFIG = build_scrape_config()
else:
    SCRAPE_CONFIG = {
        "DOWNLOAD_DELAY": 0.35,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": frozenset({408, 429, 500, 502, 503, 504, 522, 524, 403}),
        "DOMAIN_DOWNLOAD_DELAY": {
            "www.spglobal.com": 0.6,
            "www.rolandberger.com": 0.45,
        },
        "ASYNC_STEP_SLEEP": 0.0,  # 并发模式下无需步进等待
    }

_DOMAIN_LAST_REQUEST_MONO: dict[str, float] = {}

# HTTP 会话复用（连接池 / Keep-Alive），对齐 curl_cffi / requests 社区常见写法
_CURL_SESSIONS_BY_IMP: dict[str, object] = {}
_REQUESTS_SESSION = None


def _get_curl_session(impersonate: str):
    if impersonate not in _CURL_SESSIONS_BY_IMP:
        _CURL_SESSIONS_BY_IMP[impersonate] = _req.Session()
    return _CURL_SESSIONS_BY_IMP[impersonate]


def _get_requests_session():
    global _REQUESTS_SESSION
    if _REQUESTS_SESSION is None:
        _REQUESTS_SESSION = _req.Session()
    return _REQUESTS_SESSION


def html_to_soup(html: str) -> BeautifulSoup:
    """优先 lxml 解析（更快），否则回退 html.parser。"""
    if not html:
        return BeautifulSoup("", "html.parser")
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def _effective_download_delay(netloc: str) -> float:
    """返回该域名基础延迟（秒）。"""
    host = (netloc or "").lower()
    default = float(SCRAPE_CONFIG.get("DOWNLOAD_DELAY") or 0.0)
    if not host:
        return default
    per = SCRAPE_CONFIG.get("DOMAIN_DOWNLOAD_DELAY") or {}
    if host in per:
        return float(per[host])
    if host.startswith("www.") and host[4:] in per:
        return float(per[host[4:]])
    return default


def _throttle_domain_for_url(url: str) -> None:
    """
    在发起同步 HTTP 请求前，对同一 netloc 做礼貌间隔（Scrapy Downloader Slot 思路）。
    """
    base = _effective_download_delay((urlparse(url).netloc or "").lower())
    if base <= 0:
        return
    host = (urlparse(url).netloc or "").lower()
    if not host:
        return
    if SCRAPE_CONFIG.get("RANDOMIZE_DOWNLOAD_DELAY", True):
        need = random.uniform(0.5 * base, 1.5 * base)  # noqa: S311
    else:
        need = base
    now = time.monotonic()
    last = _DOMAIN_LAST_REQUEST_MONO.get(host, 0.0)
    elapsed = now - last
    if elapsed < need:
        time.sleep(need - elapsed)
    _DOMAIN_LAST_REQUEST_MONO[host] = time.monotonic()


# ════════════════════════════════════════════════════
# 请求头生成器
# ════════════════════════════════════════════════════
def make_headers(for_browser=False):
    """生成真实浏览器请求头"""
    if BROWSERFORGE_AVAILABLE:
        try:
            gen = HeaderGenerator(
                browser=[Browser(name="chrome", min_version=120, max_version=125)],
                os=("macos", "windows"),
                device="desktop"
            )
            h = gen.generate()
            if for_browser:
                # 浏览器模式只保留部分头
                return {k: v for k, v in h.items()
                        if k.lower() in ("user-agent", "accept-language", "accept-encoding")}
            return h
        except Exception: pass

    # 降级：固定请求头
    agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    ]
    return {
        "User-Agent": random.choice(agents),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    }

# ════════════════════════════════════════════════════
# HTTP 请求（curl_cffi 优先）
# ════════════════════════════════════════════════════
def http_get(url, headers=None, timeout=20, retries=None):
    """
    智能 HTTP GET：curl_cffi TLS 指纹伪装 > requests
    重试与限速策略对齐 Scrapy：RETRY_HTTP_CODES + 指数退避 + 按域名 DOWNLOAD_DELAY。
    """
    h = headers or make_headers()
    if retries is None:
        retries = int(SCRAPE_CONFIG.get("RETRY_TIMES") or 3)
    retry_codes = SCRAPE_CONFIG.get("RETRY_HTTP_CODES") or frozenset()
    # curl_cffi 可用时轮换多种浏览器指纹
    impersonates = ["chrome124", "chrome120", "chrome110", "safari17_0", "edge122"]

    for attempt in range(retries):
        _throttle_domain_for_url(url)
        try:
            if CURL_AVAILABLE:
                imp = impersonates[attempt % len(impersonates)]
                sess = _get_curl_session(imp)
                r = sess.get(url, headers=h, timeout=timeout, impersonate=imp)
            else:
                r = _get_requests_session().get(url, headers=h, timeout=timeout)

            if r.status_code == 200:
                return r
            if r.status_code in retry_codes and attempt < retries - 1:
                msg = f"HTTP {r.status_code} (尝试 {attempt+1}/{retries})"
                print(f"         ⚠ {msg}")
                _log.warning("%s %s", msg, url[:120])
                time.sleep(min(2 ** attempt, 20))
                continue
            return r
        except Exception as e:
            if attempt < retries - 1:
                print(f"         ⚠ 请求异常 (尝试 {attempt+1}/{retries}): {e}")
                _log.warning("请求异常 %s: %s", url[:120], e)
                if CURL_AVAILABLE and ("HTTP/2" in str(e) or "stream" in str(e)):
                    try:
                        import requests as _std_rq_try
                        _fb_try = _std_rq_try.get(url, headers=h, timeout=timeout)
                        if _fb_try.status_code == 200:
                            print(f"         ✓ 标准 requests 兜底成功")
                            return _fb_try
                    except Exception:
                        pass
                time.sleep(min(2 ** attempt, 20))
            else:
                print(f"         ⚠ 请求失败: {e}")
                _log.error("请求失败 %s: %s", url[:120], e)
                # curl_cffi HTTP/2 错误时，最终用标准 requests 兜底
                if CURL_AVAILABLE and ("HTTP/2" in str(e) or "stream" in str(e)):
                    try:
                        import requests as _std_rq_fb
                        _fb = _std_rq_fb.get(url, headers=h, timeout=timeout)
                        if _fb.status_code == 200:
                            print(f"         ✓ 标准 requests 兜底成功")
                            return _fb
                    except Exception:
                        pass
    return None

# ════════════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════════════
def reg_font():
    for p in ["C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/arial.ttf",
              "C:/Windows/Fonts/calibri.ttf", "/System/Library/Fonts/Helvetica.ttc",
              "/Library/Fonts/Arial.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]:
        if os.path.exists(p):
            try:
                pdfmetrics.registerFont(TTFont("F", p))
                return "F"
            except Exception: pass
    return "Helvetica"

def get_company_name(url):
    netloc = urlparse(url).netloc.lower()
    # 品牌名特殊处理（用于文件命名显示更准确）
    overrides = {
        "rolandberger.com": "RolandBerger",
        "rolandberger-com.mynewsdesk.com": "RolandBerger",
    }
    for host, name in overrides.items():
        if netloc == host or netloc.endswith("." + host):
            return name
    if "mynewsdesk.com" in netloc:
        sub = netloc.split(".mynewsdesk.com")[0]
        sub = re.sub(r'-(com|de|en|fr|uk|us|cn)$', '', sub)
        return "".join(p.capitalize() for p in sub.replace("-", " ").split())
    domain = netloc
    for pfx in ["www.", "news.", "media.", "newsroom.", "press.", "investor."]:
        if domain.startswith(pfx):
            domain = domain[len(pfx):]
    raw = domain.split(".")[0]
    for sfx in ["-com","-de","-en","-news","-newsroom","-media","-press","-group","-ag","-gmbh"]:
        raw = raw.replace(sfx, "")
    return "".join(p.capitalize() for p in re.split(r'[-_]', raw) if p)

def make_filename(company, title, date, ext):
    clean = re.sub(r'[\\/*?:"<>|]', "", title or "")
    clean = re.sub(r'\s+', '-', clean.strip())[:80].rstrip('-')
    # date 为空时用 unknown-date，不用今天日期，避免文件名误导
    if date and re.match(r'\d{4}-\d{2}-\d{2}', date):
        d = date.replace("-", ".")
    else:
        d = "unknown-date"
    return f"[{company}]{clean}-{d}.{ext}"

def parse_date(text):
    if not text: return None
    text = re.sub(r'\s+', ' ', str(text).strip())[:200]
    # 剥离 "Category / Article / " 前缀（kearney格式）
    text = re.sub(r'^.*?/\s*', '', text).strip()

    # 先从任意字符串中抽取 ISO 日期（最稳，兼容 2026-03-05T12:34:56Z）
    m = re.search(r'\b(20\d{2})-(\d{2})-(\d{2})\b', text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass

    # 兼容 2026/3/5、2026.3.5
    m = re.search(r'\b(20\d{2})[./\-](\d{1,2})[./\-](\d{1,2})\b', text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass

    for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%B %d, %Y", "%B %d %Y",
                "%d %B %Y", "%b %d, %Y", "%b %d %Y", "%d. %B %Y",
                "%d %b %Y", "%Y/%m/%d", "%m/%d/%Y"]:
        try:
            d = datetime.strptime(text[:25].strip(), fmt)
            if 2000 <= d.year <= 2100: return d
        except Exception: pass
    # Month DD, YYYY
    m = re.search(r'(January|February|March|April|May|June|July|'
                  r'August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})', text, re.I)
    if m:
        try: return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%B %d %Y")
        except Exception: pass
    # DD Month YYYY
    m = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|'
                  r'August|September|October|November|December)\s+(20\d{2})', text, re.I)
    if m:
        try: return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
        except Exception: pass
    # YYYY-MM-DD
    m = re.search(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', text)
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception: pass
    # DD/MM/YYYY (Porsche格式)
    m = re.search(r'(\d{1,2})[./](\d{1,2})[./](20\d{2})', text)
    if m:
        try: return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except Exception: pass
    return None

# ════════════════════════════════════════════════════
# S&P Global Mobility (Automotive Insights) 专属：API 列表抓取
# 说明：该站点对 Playwright/自动化请求会返回 Akamai 403，
#      但 curl_cffi 指纹伪装可正常访问其搜索 API。
# ════════════════════════════════════════════════════
def _spg_build_fq_or(field, values):
    """构建 Solr fq: field:(\"a\" OR \"b\")"""
    vs = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        s = s.replace('\\', '\\\\').replace('"', '\\"')
        vs.append(f"\"{s}\"")
    if not vs:
        return None
    if len(vs) == 1:
        return f'{field}:{vs[0]}'
    return f'{field}:(' + ' OR '.join(vs) + ')'


def fetch_spglobal(original_url, months, cutoff):
    """
    通过 S&P Global 的 query API 拉取 Automotive Insights 博客列表。
    支持从 URL hash 中读取 rows/pagenum/sort/facets。

    【修复说明 v5.1】
    Bug-1: URL 中 # 后面的参数包含 %7B...%7D（编码的花括号），
           urlparse.fragment 拿到的是已解码字符串，但 parse_qs 要求
           key=value 格式，而 facets={"k":["v"]} 的花括号不是合法 QS，
           导致 facets_raw 解析失败 → facets={} → fq_list 只剩通用过滤器
           → 查询范围远大于预期，API 返回混有其他栏目内容，命中数虽多
           但通过 cutoff 过滤后与预期相差悬殊。
           修复：先对整个 fragment 做一次 unquote，再用正则逐字段提取。

    Bug-2: rows 从 URL 读取为 100，但 rows_i 默认值只有 50，
           当 fragment 解析失败时回落 50，导致每页只拿一半。
           修复：rows 默认值改为 100（与用户 URL 一致）。

    Bug-3: 翻页终止条件 `oldest_dt < cutoff` 过于激进：
           API 按时间降序返回，若某一页最早的一篇恰好早于 cutoff，
           就立即停止，但该页中在 cutoff 之后的文章已经被正确添加；
           问题在于下一页可能仍有处于 cutoff 窗口内的文章（API 排序
           并非严格逐条单调，同一天多篇时顺序可乱）。
           修复：改为"连续两页都没有新增文章"才终止，确保不早退。

    Bug-4: fq 条件 `es_url_s:*automotive-insights/en/blogs*` 会把
           URL 路径中包含 /blogs/ 子目录（如 /blogs/detail/xxx）的文章
           全部包含，但同时也会漏掉部分 URL 格式为 /en/blogs/xxx 而非
           /en/blogs/detail/xxx 的文章（wildcard 两端有 * 应无问题，
           保留不改），主要问题是 Whitepaper 类内容的 URL 有时不含
           /blogs/ 路径，把这条 fq 改为宽松的 division 过滤即可让
           facets 中的 es_content_type_s 来精确限定类型。
    """
    base_query = "https://www.spglobal.com/api/apps/spglobal-prod/query/spglobal-prod"
    token_ep = "https://www.spglobal.com/content/spglobal/api/servlets/searchAuthToken.generate.json"

    # ── Bug-1 修复：robust fragment 解析 ──────────────────────────────
    raw_frag = urlparse(original_url).fragment or ""
    # fragment 可能已经是解码后的字符串，也可能部分编码，统一 unquote 一次
    decoded_frag = unquote(raw_frag)

    def _extract_param(name, text, default=""):
        """从 key=value&... 字符串中提取指定 key 的值（支持 JSON value）。
        先尝试标准 parse_qs；失败时用 & 分割逐段匹配（兼容 value 含花括号）。
        """
        try:
            qp = parse_qs(text, keep_blank_values=True)
            if name in qp:
                return qp[name][0]
        except Exception:
            pass
        # 兜底：按 & 分割，找到 name= 开头的段，其后直到下一个已知参数键
        known_keys = {"q", "rows", "pagenum", "sort", "facets"}
        parts = text.split("&")
        for i, part in enumerate(parts):
            if part.startswith(name + "="):
                val = part[len(name) + 1:]
                # 若后续 part 不以已知 key= 开头，说明 & 被当作 JSON 内容分割了，拼回来
                j = i + 1
                while j < len(parts):
                    nxt = parts[j]
                    if any(nxt.startswith(k + "=") for k in known_keys):
                        break
                    val += "&" + nxt
                    j += 1
                return val
        return default

    q          = _extract_param("q",       decoded_frag, "").strip()
    rows       = _extract_param("rows",    decoded_frag, "").strip()
    pagenum    = _extract_param("pagenum", decoded_frag, "").strip()
    sort       = _extract_param("sort",    decoded_frag, "").strip()
    facets_raw = _extract_param("facets",  decoded_frag, "").strip()

    # ── Bug-2 修复：rows 默认值改为 100 ──────────────────────────────
    try:
        rows_i = int(rows) if rows else 100
    except Exception:
        rows_i = 100
    try:
        page_i = int(pagenum) if pagenum else 1
    except Exception:
        page_i = 1

    sort = sort if sort else "es_unified_dt desc"
    q = q if q else "*:*"

    facets = {}
    if facets_raw:
        try:
            facets = json.loads(facets_raw)
            print(f"  ✓ facets 解析成功: {list(facets.keys())}")
        except Exception as e:
            print(f"  ⚠ facets JSON 解析失败({e})，尝试宽松解析: {facets_raw[:120]}")
            # 尝试修复不严格 JSON（单引号 → 双引号）
            try:
                facets = json.loads(facets_raw.replace("'", '"'))
                print(f"  ✓ facets 宽松解析成功: {list(facets.keys())}")
            except Exception:
                facets = {}

    def _new_session():
        if CURL_AVAILABLE:
            return _req.Session()
        import requests as _rq
        return _rq.Session()

    def _warmup_and_token(sess, imp):
        blog_page = "https://www.spglobal.com/automotive-insights/en/blogs"
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        page_h = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        token_h = {
            "User-Agent": ua,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": blog_page,
            "Origin": "https://www.spglobal.com",
            "Connection": "keep-alive",
        }
        try:
            if CURL_AVAILABLE:
                sess.get(blog_page, headers=page_h, timeout=25, impersonate=imp)
                tr = sess.get(token_ep, headers=token_h, timeout=25, impersonate=imp)
            else:
                sess.get(blog_page, headers=page_h, timeout=25)
                tr = sess.get(token_ep, headers=token_h, timeout=25)
            if tr.status_code != 200:
                return "", tr.status_code
            try:
                t = (tr.json() or {}).get("token", "") or ""
            except Exception:
                t = ""
            return t, 200 if t else 0
        except Exception:
            return "", 0

    # 初次拿 token（用 warmup session）
    impersonates = ["chrome124", "chrome120", "edge122", "safari17_0"]
    sess0 = _new_session()
    token = ""
    backoff = 1.5
    for attempt in range(6):
        imp = impersonates[attempt % len(impersonates)]
        t, code = _warmup_and_token(sess0, imp)
        if t:
            token = t
            break
        if code in (403, 429, 503, 0):
            print(f"         ⚠ token HTTP {code} (尝试 {attempt+1}/6)")
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 12)
            sess0 = _new_session()
            continue
        print(f"         ⚠ token HTTP {code} (尝试 {attempt+1}/6)")
        time.sleep(backoff)
        backoff = min(backoff * 1.5, 10)
        sess0 = _new_session()

    if not token:
        print("  ⚠ 无法获取 spglobal token")
        return []

    # query headers（稳定最小头 + Bearer）
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.spglobal.com/automotive-insights/en/blogs",
        "Origin": "https://www.spglobal.com",
        "Connection": "keep-alive",
        "Authorization": f"Bearer {token}",
    }

    # fq 策略（基于实测诊断结果）：
    #   - es_url_s:*spglobal.com/automotive-insights*  → numFound=0，完全失效，禁用
    #   - es_url_s:*automotive-insights/en/blogs*       → numFound=116（有效但漏 Whitepaper）
    #   - division + theme + content_type（无 url fq）  → numFound=124（覆盖最全）
    # 结论：去掉 es_url_s 过滤，依靠 division + facets 精确限定，覆盖文章数最多
    fq_list = [
        'es_division_s:"S&P Global Mobility"',
    ]

    if isinstance(facets, dict):
        for k, vs in facets.items():
            if not vs:
                continue
            fq = _spg_build_fq_or(k, vs if isinstance(vs, list) else [vs])
            if fq:
                fq_list.append(fq)
                print(f"    fq += {fq[:100]}")
    else:
        print("  ⚠ facets 为空，将抓取全部内容类型")

    articles = []
    seen = set()
    start = max(page_i - 1, 0) * rows_i
    max_pages = 30
    page_count = 0
    # ── Bug-3 修复：用"连续空页"计数替代单页 oldest_dt 立即退出 ────────
    consecutive_empty = 0
    MAX_CONSECUTIVE_EMPTY = 2

    print(f"\n  [S&P Global] API 抓取列表（rows={rows_i}, start={start}, sort={sort}）")
    print(f"  fq 条件共 {len(fq_list)} 条")

    while page_count < max_pages:
        page_count += 1
        params = [("q", q), ("rows", str(rows_i)), ("start", str(start)), ("sort", sort)]
        for fq in fq_list:
            params.append(("fq", fq))

        r = None
        sess = sess0  # 复用 warmup session/cookies
        backoff = 1.2
        for attempt in range(6):
            imp = impersonates[attempt % len(impersonates)]
            try:
                if CURL_AVAILABLE:
                    r = sess.get(base_query, params=params, headers=headers, timeout=25, impersonate=imp)
                else:
                    r = sess.get(base_query, params=params, headers=headers, timeout=25)
            except Exception:
                r = None

            status = r.status_code if r is not None else 0
            if status == 200:
                break
            if status in (401, 403, 429, 503):
                # 重建 session + 刷新 token
                sess = _new_session()
                t, _ = _warmup_and_token(sess, imp)
                if t:
                    headers["Authorization"] = f"Bearer {t}"
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 10)
                continue
            break

        if not r or r.status_code != 200:
            print(f"  ⚠ spglobal query HTTP {r.status_code if r else 0}")
            break

        try:
            data = r.json()
        except Exception:
            print("  ⚠ spglobal query 返回非 JSON")
            break

        resp = data.get("response") or {}
        docs = resp.get("docs") or []
        num_found = resp.get("numFound")
        if not docs:
            break

        # ── Bug-3 修复：记录本页所有文章的 oldest_dt，但不立即退出 ──────
        oldest_dt = None
        added = 0
        all_before_cutoff = True  # 本页所有文章是否都早于 cutoff
        for d in docs:
            url = d.get("es_url_s") or ""
            title = (d.get("es_title_t") or "").strip()
            dt_raw = d.get("es_unified_dt") or ""
            if not url or not title:
                continue
            if not url.startswith("http"):
                url = urljoin("https://www.spglobal.com", url)
            # 去重：统一 scheme/host，去掉 query/hash，去尾部 /
            try:
                pu = urlparse(url)
                canon = f"https://{pu.netloc}{pu.path}".rstrip("/")
            except Exception:
                canon = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
            if canon in seen:
                continue
            seen.add(canon)

            pub_dt = parse_date(dt_raw) if dt_raw else None
            date_str = pub_dt.strftime("%Y-%m-%d") if pub_dt else ""
            if pub_dt:
                oldest_dt = pub_dt if oldest_dt is None else min(oldest_dt, pub_dt)
                if pub_dt >= cutoff:
                    all_before_cutoff = False  # 至少有一篇在 cutoff 内
                if pub_dt < cutoff:
                    continue  # 跳过该篇，但继续处理本页其余文章

            articles.append({
                "url": url,
                "title": title[:200],
                "date": date_str,
                "pub_date": pub_dt,
                "summary": (d.get("es_description_t") or "").strip()[:500],
                "source": "spglobal",
                "_fallback_body_txt": (d.get("es_body_content_txt") or [])[:40],
                "_spg_description": (d.get("es_description_t") or "").strip(),
                "_spg_excerpt": (d.get("es_excerpt_t") or d.get("es_content_t") or "").strip()[:2000],
                "_spg_ctype": (d.get("es_content_type_s") or "").strip(),
                "_spg_themes": d.get("es_theme_ss") or [],
            })
            added += 1

        print(f"  → start={start}  本页新增:{added}  累计:{len(articles)}"
              + (f"  oldest={oldest_dt.strftime('%Y-%m-%d') if oldest_dt else 'N/A'}"
                 f"  numFound={num_found}"))

        # ── Bug-3 修复：仅当本页全部文章都早于 cutoff 时才退出 ───────────
        if oldest_dt and all_before_cutoff:
            print(f"  → 本页所有文章均早于 cutoff，停止翻页")
            break

        if added == 0:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"  → 连续 {consecutive_empty} 页无新增，停止翻页")
                break
        else:
            consecutive_empty = 0

        try:
            if isinstance(num_found, int) and start + rows_i >= num_found:
                print(f"  → 已到达 numFound={num_found} 末尾，停止翻页")
                break
        except Exception:
            pass

        start += rows_i
        time.sleep(0.4)

    articles.sort(key=lambda x: x["date"] or "", reverse=True)
    print(f"  [S&P Global] 共 {len(articles)} 篇（时间范围内）")
    return articles

# ════════════════════════════════════════════════════
# Roland Berger 专属：requests 直接解析列表页
# ════════════════════════════════════════════════════
def fetch_rolandberger(months, cutoff):
    base = "https://rolandberger-com.mynewsdesk.com/pressreleases"
    # 精确匹配文章 URL：末尾必须有 5 位以上数字 ID
    pat = re.compile(
        r'^https?://rolandberger-com\.mynewsdesk\.com/pressreleases/[a-z0-9][a-z0-9\-]+-\d{5,}$',
        re.IGNORECASE)
    articles = []
    seen = set()
    url = base
    pg = 1
    print(f"\n  [Roland Berger] 抓取列表页（curl_cffi={'是' if CURL_AVAILABLE else '否'}）")

    while True:
        print(f"  → 第{pg}页: {url}")
        r = http_get(url)
        if not r: break

        soup = html_to_soup(r.text)
        found = 0
        stop = False

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"): href = "https://rolandberger-com.mynewsdesk.com" + href
            elif not href.startswith("http"): continue
            if not pat.match(href) or href in seen: continue
            seen.add(href)

            title = a.get("title") or a.get_text(strip=True)
            if len(title) < 10: continue

            # 日期：往上找 8 层父容器
            date_str = ""
            pub_date = None
            container = a
            for _ in range(8):
                container = container.parent
                if not container: break
                dm = re.search(
                    r'(\d{1,2})\s+(January|February|March|April|May|June|July|'
                    r'August|September|October|November|December)\s+(20\d{2})',
                    container.get_text(" ", strip=True), re.I)
                if dm:
                    try:
                        pub_date = datetime.strptime(
                            f"{dm.group(1)} {dm.group(2)} {dm.group(3)}", "%d %B %Y")
                        date_str = pub_date.strftime("%Y-%m-%d")
                        break
                    except Exception: pass

            if pub_date and pub_date < cutoff:
                stop = True
                break

            articles.append({
                "url": href, "title": title[:200],
                "date": date_str, "pub_date": pub_date,
                "summary": "", "source": "rolandberger",
            })
            found += 1

        print(f"    新增:{found}  累计:{len(articles)}")
        if stop or found == 0: break

        sm = soup.find("a", href=re.compile(r"after="))
        if sm:
            h = sm["href"]
            url = ("https://rolandberger-com.mynewsdesk.com" + h) if h.startswith("/") else h
            pg += 1
            time.sleep(0.5)
        else:
            break

    articles.sort(key=lambda x: x["date"] or "", reverse=True)
    print(f"  [Roland Berger] 共 {len(articles)} 篇")
    return articles

# ════════════════════════════════════════════════════
# Porsche Consulting 专属：从 Porsche Newsroom 抓取
# ════════════════════════════════════════════════════
def fetch_porsche_consulting(months, cutoff):
    """
    Porsche Consulting 文章实际在 newsroom.porsche.com
    日期格式: DD/MM/YYYY
    """
    base_url = "https://newsroom.porsche.com/en/company/porsche-consulting.html"
    articles = []
    seen = set()

    print(f"  [Porsche Consulting] 抓取: {base_url}")
    r = http_get(base_url)
    if not r:
        print("  ⚠ 无法访问 Porsche Newsroom")
        return []

    soup = html_to_soup(r.text)

    # 文章链接格式: /en/YYYY/company/porsche-consulting-xxx.html
    article_pattern = re.compile(
        r'^https?://newsroom[.]porsche[.]com/en/[0-9]{4}/company/porsche-consulting[^"]*[.]html$',
        re.IGNORECASE)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = "https://newsroom.porsche.com" + href
        if not article_pattern.match(href) or href in seen:
            continue
        seen.add(href)

        title = a.get_text(strip=True)
        if len(title) < 5:
            # 从父容器找标题
            p = a.parent
            for _ in range(4):
                if p is None: break
                t = p.get_text(strip=True)
                if 10 < len(t) < 200:
                    title = t; break
                p = p.parent

        # 日期：找 "DD/MM/YYYY" 格式
        date_str = ""
        pub_date = None
        container = a
        for _ in range(6):
            container = container.parent
            if not container: break
            txt = container.get_text(" ", strip=True)
            dm = re.search(r'(\d{1,2})/(\d{1,2})/(20\d{2})', txt)
            if dm:
                try:
                    pub_date = datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)))
                    date_str = pub_date.strftime("%Y-%m-%d")
                    break
                except Exception: pass

        if pub_date and pub_date < cutoff:
            continue

        articles.append({
            "url": href, "title": title[:200],
            "date": date_str, "pub_date": pub_date,
            "summary": "", "source": "porsche_consulting",
        })

    articles.sort(key=lambda x: x["date"] or "", reverse=True)
    print(f"  [Porsche Consulting] 共 {len(articles)} 篇")
    return articles


# ════════════════════════════════════════════════════
# Porsche Consulting (news-trends) 专属：分页列表抓取
# ════════════════════════════════════════════════════
def fetch_porsche_news_trends(list_url, cutoff):
    from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

    u = urlsplit(list_url)
    base_q = dict(parse_qsl(u.query, keep_blank_values=True))
    base_q.pop("page", None)

    articles = []
    seen_urls = set()
    seen_page_sigs = set()
    max_pages = 20
    no_new_count = 0

    print(f"  [Porsche Consulting] news-trends 分页抓取: {list_url}")

    for i in range(max_pages):
        page_val = ("," * 10) + str(i)
        q = dict(base_q)
        q["page"] = page_val
        page_url = urlunsplit((u.scheme, u.netloc, u.path, urlencode(q, doseq=True), u.fragment))
        r = http_get(page_url, timeout=25, retries=3)
        if not r or r.status_code != 200:
            break

        soup = html_to_soup(r.text)

        # 三种内容路径：article / publication / porsche-consulting-the-magazine
        _PC_CONTENT_SEL = (
            'a[href^="/international/en/article/"],'
            'a[href^="/international/en/publication/"],'
            'a[href^="/international/en/porsche-consulting-the-magazine/"]'
        )

        # 检测重复页面（超出范围时网站返回同样内容）
        page_hrefs = frozenset(
            a.get("href","") for a in soup.select(_PC_CONTENT_SEL)
            if a.get("href")
        )
        if page_hrefs in seen_page_sigs and page_hrefs:
            print(f"  → page={i} 检测到重复页面，停止")
            break
        if page_hrefs:
            seen_page_sigs.add(page_hrefs)

        found = 0
        for a in soup.select(_PC_CONTENT_SEL):
            href = a.get("href") or ""
            title = a.get_text(" ", strip=True)
            if not href or not title or len(title) < 6:
                continue
            full = urljoin("https://www.porsche-consulting.com", href)
            if full in seen_urls:
                continue
            seen_urls.add(full)
            articles.append({
                "url": full, "title": title[:200],
                "date": "", "pub_date": None,
                "summary": "", "source": "porsche_news_trends",
            })
            found += 1

        print(f"  → page={i} 新增:{found} 累计:{len(articles)}")

        if found == 0:
            no_new_count += 1
            if no_new_count >= 2: break
        else:
            no_new_count = 0
        time.sleep(0.3)

    # ── 批量并发获取文章日期（Porsche Consulting 日期只在详情页）──────────
    # 日期格式：DD.MM.YYYY（如 23.03.2026）
    print(f"  → 批量获取 {len(articles)} 篇文章日期...")
    import concurrent.futures as _cf
    _date_h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"}

    _MONTH_MAP = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                   "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

    def _fetch_porsche_date(url):
        try:
            r = http_get(url, headers=_date_h, timeout=10)
            if not r or r.status_code != 200:
                return url, None
            from datetime import datetime as _dt
            # 格式 1：DD.MM.YYYY（article / magazine 页面）
            dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(20\d{2})', r.text)
            if dm:
                return url, _dt(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)))
            # 格式 2：Mon YYYY（publication 页面，如 "Mar 2026 | Report"）
            mm = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(20\d{2})',
                           r.text[:80000], re.IGNORECASE)
            if mm:
                mon = _MONTH_MAP.get(mm.group(1)[:3].lower())
                yr = int(mm.group(2))
                if mon:
                    return url, _dt(yr, mon, 1)
            return url, None
        except Exception:
            return url, None

    with _cf.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_porsche_date, a["url"]): a for a in articles}
        for future in _cf.as_completed(futures):
            url_r, pub_dt = future.result()
            art = futures[future]
            if pub_dt:
                art["pub_date"] = pub_dt
                art["date"] = pub_dt.strftime("%Y-%m-%d")

    # 按日期过滤
    before = len(articles)
    articles = [a for a in articles
                if a["pub_date"] is None or a["pub_date"] >= cutoff]
    print(f"  → 日期过滤：{before} → {len(articles)} 篇（cutoff={cutoff.strftime('%Y-%m-%d')}）")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [Porsche Consulting] news-trends 共 {len(articles)} 篇")
    return articles


async def fetch_rolandberger_site(url, cutoff):
    links = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=make_headers()["User-Agent"],
            locale="en-US",
        )
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2500)

        for sel in ["button:has-text('Accept')", "button:has-text('Accept All')", "button:has-text('OK')"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=800):
                    await btn.click()
                    await page.wait_for_timeout(600)
                    break
            except Exception:
                pass

        for _ in range(12):
            await page.evaluate("window.scrollTo(0,document.body.scrollHeight)")
            await page.wait_for_timeout(1200)
            for sel in ["button:has-text('Load More')", "button:has-text('Load more')", "a:has-text('Load More')"]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=600):
                        await btn.click()
                        await page.wait_for_timeout(1500)
                        break
                except Exception:
                    pass

        # 优先从 Publications 区块抓链接；找不到再退回 main 全文
        items = await page.evaluate(
            """() => {
                const main = document.querySelector('main') || document.body;
                // 找到“Publications”标题附近的容器
                const all = Array.from(main.querySelectorAll('*'));
                const pubHeader = all.find(el => {
                    const t = (el.innerText || '').trim();
                    return t === 'Publications';
                });
                let root = null;
                if (pubHeader) {
                    root = pubHeader.closest('section') || pubHeader.parentElement;
                    // 向上找一个包含较多链接的容器
                    for (let i = 0; i < 6 && root; i++) {
                        const links = root.querySelectorAll('a[href]').length;
                        if (links >= 8) break;
                        root = root.parentElement;
                    }
                }
                root = root || main;
                const out = [];
                const anchors = Array.from(root.querySelectorAll('a[href]'));
                for (const a of anchors) {
                    const href = a.getAttribute('href') || '';
                    const text = (a.innerText || '').trim().replace(/\\s+/g,' ');
                    out.push({href, text});
                }
                return out;
            }"""
        )
        await browser.close()

    for it in items:
        href = it.get("href") or ""
        text = (it.get("text") or "").strip()
        if not href or href.startswith("#"):
            continue

        full = href if href.startswith("http") else urljoin("https://www.rolandberger.com", href)
        if "rolandberger.com" not in urlparse(full).netloc.lower():
            continue

        low = full.lower()
        if any(x in low for x in ["privacy", "cookies", "jobs", "contact", "legal", "login", "newsletter"]):
            continue

        # 只保留 Insights 里的“内容页”，过滤 Global Topics/栏目/导航/搜索页
        if "/en/insights/" not in low:
            continue
        if any(x in low for x in [
            "/en/insights/global-topics",
            "/en/insights/all-",
            "/en/insights/search",
            "/en/insights/newsroom",
            "/en/insights/newsletter",
        ]):
            continue
        # 不再用“末尾 /”判断是否目录页：该站不少详情页也可能以 / 结尾

        if len(text) < 8:
            continue

        # canonicalize：去掉 query/hash，并统一去尾部 /
        key = full.split("#", 1)[0].split("?", 1)[0].rstrip("/")
        if key in seen:
            continue

        seen.add(key)
        links.append(
            {
                "url": key,
                "title": text[:200],
                "date": "",
                "pub_date": None,
                "summary": "",
                "source": "rolandberger_site",
            }
        )

    links.sort(key=lambda x: x["title"] or "")
    print(f"  [Roland Berger] site 共 {len(links)} 篇（未按日期过滤）")
    return links
# ════════════════════════════════════════════════════
# Schaeffler 专属：已验证文章列表
# ════════════════════════════════════════════════════
def fetch_schaeffler(months, cutoff):
    PR = "https://www.schaeffler.com/en/media/press-releases/press-releases-detail.jsp?id="
    IR = "https://www.schaeffler.com/en/investor-relations/events-publications/ir-releases/ir_releases_detail.jsp?id="
    known = {
        "88176003": ("2026-03-05", "Schaeffler AG shares move up to MDAX", PR),
        "88176002": ("2026-03-05", "Schaeffler receives VDA Logistics Award", PR),
        "88175106": ("2026-03-03", "Schaeffler reports solid results for 2025 (Press)", PR),
        "88175301": ("2026-03-03", "Schaeffler reports solid results for 2025 (IR)", IR),
        "88175299": ("2026-03-03", "Schaeffler publishes 2025 Sustainability Statement", PR),
        "88174976": ("2026-03-02", "Humanoid robotics: Schaeffler and Leju Robotics", PR),
        "88174722": ("2026-02-27", "Changes on Executive Board of Schaeffler AG", PR),
        "88173800": ("2026-02-18", "Schaeffler Capital Markets Day 2026", PR),
        "88161152": ("2026-01-16", "Schaeffler set to head ReDriveS project", PR),
        "88159744": ("2026-01-13", "Schaeffler and Humanoid partnership (Press)", PR),
        "88159810": ("2026-01-13", "Schaeffler and Humanoid partnership (IR)", IR),
        "88156672": ("2026-01-02", "Schaeffler planetary gear actuator CES 2026", PR),
        "88158785": ("2026-01-02", "Schaeffler motion technology CES 2026", PR),
        "88155000": ("2025-12-02", "Schaeffler CTI Berlin electrification", PR),
        "88159168": ("2025-12-01", "Schaeffler CDP Climate A List 2025", PR),
        "88152642": ("2025-11-28", "Schaeffler NTU Singapore humanoid", PR),
        "88136515": ("2025-11-04", "Schaeffler and Neura Robotics partnership", PR),
        "88136385": ("2025-11-04", "Schaeffler 9M 2025 results", PR),
        "88136517": ("2025-11-04", "Schaeffler sells turbocharger China", PR),
        "88131000": ("2025-10-28", "Schaeffler raises guidance cashflow 2025", PR),
        "88124864": ("2025-09-16", "Schaeffler Capital Markets Day 2025", PR),
    }

    # ── 硬编码列表过期检测 ─────────────────────────────────────
    # 若已知列表中最新文章距今超过 30 天，说明列表需要更新，打印醒目警告。
    try:
        _newest_date = max(
            datetime.strptime(v[0], "%Y-%m-%d") for v in known.values()
        )
        _days_stale = (datetime.now() - _newest_date).days
        if _days_stale > 30:
            print(
                f"\n  ⚠️  [Schaeffler] 硬编码列表已过期 {_days_stale} 天！"
                f"（最新条目: {_newest_date.strftime('%Y-%m-%d')}）"
                f"\n     请更新 fetch_schaeffler() 中的 known 字典，否则近期新闻将被遗漏。\n"
            )
            _log.warning(
                "Schaeffler known list is %d days stale (newest: %s). "
                "Update fetch_schaeffler() known dict.",
                _days_stale, _newest_date.strftime("%Y-%m-%d"),
            )
    except Exception:
        pass
    # ───────────────────────────────────────────────────────────
    out = []
    for aid, (date, title, base) in known.items():
        try:
            if datetime.strptime(date, "%Y-%m-%d") < cutoff: continue
        except Exception: pass
        out.append({
            "url": base + aid, "title": title, "date": date,
            "pub_date": datetime.strptime(date, "%Y-%m-%d"),
            "summary": "", "source": "schaeffler",
        })
    print(f"  [Schaeffler] 共 {len(out)} 篇（时间范围内）")
    return out

# ════════════════════════════════════════════════════
# 通用网站：Playwright 抓取
# ════════════════════════════════════════════════════
async def fetch_generic(url, cutoff):
    """通用抓取：Playwright 渲染 + 滚动加载 + 日期提取"""
    articles = []
    seen = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=make_headers()["User-Agent"], locale="en-US")
        page = await ctx.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        except Exception:
            await page.goto(url, wait_until="load", timeout=45000)
        await page.wait_for_timeout(3000)
        for sel in ["button:has-text('Accept All')", "button:has-text('Accept all')",
                    "button:has-text('Accept')", "button:has-text('OK')",
                    "button:has-text('Agree')",
                    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                    "[data-cmp-action='acceptAll']"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=800):
                    await btn.click(); await page.wait_for_timeout(800); break
            except Exception: pass
        for _ in range(8):
            await page.evaluate("window.scrollTo(0,document.body.scrollHeight)")
            await page.wait_for_timeout(1200)
            for sel in ["button:has-text('Load more')", "button:has-text('Show more')",
                        ".load-more", "[class*='load-more']"]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=500):
                        await btn.click(); await page.wait_for_timeout(1500); break
                except Exception: pass
        html = await page.content()
        soup = html_to_soup(html)
        parsed_url = urlparse(url)
        base = parsed_url.scheme + "://" + parsed_url.netloc
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            full = href if href.startswith("http") else urljoin(base, href)
            if full in seen or len(text) < 8: continue
            if urlparse(full).netloc != parsed_url.netloc: continue
            # kearney.com 特殊格式: /industry/xxx/article/xxx
            full_low = full.lower()
            is_kearney = "kearney.com" in full_low
            if is_kearney:
                if not ("/article/" in full_low or "/insight" in full_low):
                    continue
            else:
                if not any(k in full_low for k in
                    ["press","news","release","article","insight",
                     "publication","update","blog","report"]): continue
                if any(k in full_low for k in
                    ["contact","career","about","privacy","cookie","sitemap","login"]): continue
            seen.add(full)
            date_str = ""
            pub_date = None
            container = a
            for _ in range(6):
                container = container.parent
                if not container: break
                txt = container.get_text(" ", strip=True)
                # 提取方式：先找月份+日期+年份的组合，确保 parse_date 能解析
                date_found = None
                # 格式1: "Month DD, YYYY" or "Month DD YYYY"
                m = re.search(
                    r'(January|February|March|April|May|June|July|August|'
                    r'September|October|November|December)'
                    r'\s+(\d{1,2}),?\s+(20\d{2})',
                    txt, re.IGNORECASE)
                if m:
                    date_found = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
                # 格式2: "DD Month YYYY"
                if not date_found:
                    m = re.search(
                        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|'
                        r'September|October|November|December)\s+(20\d{2})',
                        txt, re.IGNORECASE)
                    if m:
                        date_found = f"{m.group(1)} {m.group(2)} {m.group(3)}"
                # 格式3: "YYYY-MM-DD"
                if not date_found:
                    m = re.search(r'(20\d{2})[-./](\d{1,2})[-./](\d{1,2})', txt)
                    if m:
                        date_found = f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
                # 格式4: "DD.MM.YYYY" or "DD/MM/YYYY"
                if not date_found:
                    m = re.search(r'(\d{1,2})[./](\d{1,2})[./](20\d{2})', txt)
                    if m:
                        date_found = f"{m.group(1)}.{m.group(2)}.{m.group(3)}"

                if date_found:
                    d = parse_date(date_found)
                    if d and 2020 <= d.year <= 2030:
                        pub_date = d
                        date_str = d.strftime("%Y-%m-%d")
                if pub_date: break
            if pub_date and pub_date < cutoff: continue
            articles.append({"url":full,"title":text[:150],"date":date_str,"pub_date":pub_date,"summary":""})
        await browser.close()
    seen_u = set(); unique = []
    for a in articles:
        if a["url"] not in seen_u:
            seen_u.add(a["url"]); unique.append(a)
    unique.sort(key=lambda x: x["date"] or "", reverse=True)
    return unique



# ════════════════════════════════════════════════════
# Wood Mackenzie 专属：直接调内部 API
# 说明：/market-insights/ 页面通过 5 个不同 UUID 的
#      POST /api/v1/search/latest-thinking/{uuid} 接口
#      加载各板块文章；请求体 {"size":N,"from":page_num}
#      from 是页码（1起），size 最大可设 100。
# ════════════════════════════════════════════════════
# Wood Mackenzie 专属：直接调内部 API
# API 行为（实测确认）：
#   - size 参数被服务端硬忽略，始终每次返回 6 条
#   - from 是滑动偏移量（步进=1），不是页码
#     from=1 → 第1-6条，from=2 → 第2-7条（滑动窗口）
#   - 正确做法：from 每次 +6 跳过已取的6条，不重不漏
#   - 5个UUID对应不同板块，内容有重叠，需全局去重

# ════════════════════════════════════════════════════
# McKinsey 专属：ScrapingBee stealth_proxy 抓列表页
# 说明：McKinsey 使用 CF Turnstile 最高级别保护，
#      需要 stealth_proxy=True（每次消耗 75 credits）。
#      页面无限滚动，每次加载约 10-15 篇，多次翻页合并。
# ════════════════════════════════════════════════════
# McKinsey 专属：搜索引擎摘要模式
# 说明：McKinsey 使用 CF Turnstile 最高级别保护，
#      ScrapingBee stealth_proxy 每次消耗 75 credits，
#      成本太高。改用 DuckDuckGo 搜索该列表页下的文章。
# ════════════════════════════════════════════════════
def _fetch_cf_site_with_playwright(list_url, months, cutoff, site_name,
                                    link_filter_fn, scroll_count=None):
    """
    通用 patchright 深度滚动抓取 CF 保护站点的列表页。
    McKinsey / Kearney 共用此函数。
    headless=False 有一定绕过 CF JS Challenge 的成功率。
    """
    import asyncio as _aio

    if scroll_count is None:
        scroll_count = max(15, months * 3)

    print(f"\n  [{site_name}] patchright 模式（滚动{scroll_count}次）...")

    async def _do_fetch():
        try:
            from patchright.async_api import async_playwright
        except ImportError:
            from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=["--no-sandbox","--disable-blink-features=AutomationControlled"],
                slow_mo=30)
            ctx = await browser.new_context(
                viewport={"width":1440,"height":900},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                locale="en-US")
            page = await ctx.new_page()
            try:
                await page.goto(list_url, wait_until="domcontentloaded", timeout=45000)
            except Exception:
                pass
            # 等待 CF 验证通过（最多20秒）
            for i in range(20):
                await page.wait_for_timeout(1000)
                title = await page.title()
                if not any(x in title for x in ["Just a moment","Checking","Verifying"]):
                    print(f"  CF 验证通过（{i+1}秒）")
                    break
            else:
                print("  ⚠ CF 验证未通过，尝试继续抓取...")
            # 关闭 cookie 弹窗
            for sel in ["#onetrust-accept-btn-handler",
                        "button:has-text('Accept all')",
                        "button:has-text('Accept All')",
                        "button:has-text('Accept')"]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=1000):
                        await btn.click()
                        await page.wait_for_timeout(800)
                        break
                except Exception: pass
            # 深度滚动 + 点击 Load more
            prev_height = 0
            no_change_count = 0
            for i in range(scroll_count):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1200)
                for btn_sel in ["button:has-text('Load more')",
                                "button:has-text('Show more')",
                                "button:has-text('View more')",
                                "[class*='load-more']",
                                "[class*='loadMore']"]:
                    try:
                        btn = page.locator(btn_sel).first
                        if await btn.is_visible(timeout=400):
                            await btn.click()
                            await page.wait_for_timeout(1500)
                            break
                    except Exception: pass
                # 如果页面高度没有变化，说明已到底
                cur_height = await page.evaluate("document.body.scrollHeight")
                if cur_height == prev_height:
                    no_change_count += 1
                    if no_change_count >= 3:
                        print(f"  页面高度稳定，已加载完（第{i+1}次滚动）")
                        break
                else:
                    no_change_count = 0
                prev_height = cur_height
            html = await page.content()
            await browser.close()
            return html

    try:
        import concurrent.futures
        try:
            _running_loop = asyncio.get_running_loop()
        except RuntimeError:
            _running_loop = None
        if _running_loop is not None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                html = pool.submit(lambda: asyncio.run(_do_fetch())).result()
        else:
            html = loop.run_until_complete(_do_fetch())
    except Exception as e:
        print(f"  ❌ patchright 异常: {e}")
        return []

    if not html or len(html) < 5000:
        print(f"  ❌ 页面内容不足（可能被 CF 拦截）")
        return []

    print(f"  ✅ 页面获取成功（{len(html)//1024}KB）")
    soup = html_to_soup(html)
    articles = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http"):
            full_url = href.rstrip("/")
        elif href.startswith("/"):
            from urllib.parse import urlparse as _up3
            _base = _up3(list_url)
            full_url = f"{_base.scheme}://{_base.netloc}{href.rstrip('/')}"
        else:
            continue

        if not link_filter_fn(full_url):
            continue
        if full_url in seen:
            continue
        seen.add(full_url)

        # 提取标题
        title = a.get_text(strip=True)
        if len(title) < 5:
            p = a.parent
            for _ in range(4):
                if not p: break
                h = p.find(["h2","h3","h4","h5"])
                if h and len(h.get_text(strip=True)) > 5:
                    title = h.get_text(strip=True); break
                p = p.parent
        if not title:
            title = href.rstrip("/").split("/")[-1].replace("-"," ").title()

        # 提取日期
        date_str, pub_dt = "", None
        _a2 = soup.find("a", href=href)
        if _a2:
            _c = _a2
            for _ in range(6):
                _c = _c.parent
                if not _c: break
                _t = _c.find("time")
                if _t:
                    raw = _t.get("datetime","") or _t.get_text(strip=True)
                    pub_dt = parse_date(raw)
                    if pub_dt: date_str = pub_dt.strftime("%Y-%m-%d"); break
                _txt = _c.get_text(" ",strip=True)[:300]
                _dm = re.search(
                    r"(January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+\d{1,2},?\s+20\d{2}"
                    r"|20\d{2}-\d{2}-\d{2}", _txt, re.I)
                if _dm:
                    pub_dt = parse_date(_dm.group(0))
                    if pub_dt: date_str = pub_dt.strftime("%Y-%m-%d"); break

        if pub_dt and pub_dt < cutoff:
            continue

        articles.append({"url":full_url,"title":title[:200],"date":date_str,
                         "pub_date":pub_dt,"summary":"","source":site_name.lower()})

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [{site_name}] 共 {len(articles)} 篇")
    return articles


_MCK_API = "https://prd-api.mckinsey.com/api/insightsgrid/articles"
_MCK_TAXONOMY_CACHE = {
    "industries/aerospace-and-defense/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["7aa6f280-7392-4ef0-94af-f5ca6b805f81","a78d0556-eff0-44b8-8e5e-339f9f600f1d"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","c5b24078-44cd-435c-b265-4e36593a226c"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/chemicals/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["72a42c24-da14-43bf-8ee8-8aa7246fb6b2"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","ab46faae-ee8a-40f4-90f3-a8bd97619bdf","1ea27058-9b4d-4a64-81f4-f56cb6b81896","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/consumer-packaged-goods/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["fba8937a-2a6c-4a61-9024-31f29c375357","7a4da5cb-f74b-4dcc-bd96-51037f19c03c","d2e99dd2-1ffd-4b0c-9d15-18f3b5b85658"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","874ee345-9fa7-403b-bcf3-2eaf719a89fa","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","fcb448f9-af05-4682-a070-ffab859ebdb5","c3f331dd-6683-4c33-9460-e56bb1487f33","c5b24078-44cd-435c-b265-4e36593a226c","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/education/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["bdf119f9-2777-4a01-89f7-77728f084d9f"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","91cfe105-8dad-437c-8734-3d740dcdb437","15750f40-c2b7-4634-be10-763112fad183","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","1ea27058-9b4d-4a64-81f4-f56cb6b81896","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","fcb448f9-af05-4682-a070-ffab859ebdb5","c3f331dd-6683-4c33-9460-e56bb1487f33","899ef466-0f6e-4feb-965d-3882b88ac9a4","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/electric-power-and-natural-gas/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["fc046cbc-30b1-4977-96b1-6e6d920dac45","db49f8b1-3f39-4d23-af76-45d39c05a5a1","ca07d001-63ca-4118-9507-a8ed639aa6c2","5d0cfa28-fad5-463f-803d-af2e36ce0696","d4c2c5de-8d25-442d-9376-7c5f1631f9af"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","ab46faae-ee8a-40f4-90f3-a8bd97619bdf","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","a15e9534-23d8-461d-8df6-11cef71f6441","45c4660b-c2d9-49c6-9536-c259c7e988d6","96177f40-e988-4542-86f7-4b4533b0294d"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/energy-and-materials/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["72a42c24-da14-43bf-8ee8-8aa7246fb6b2","fc046cbc-30b1-4977-96b1-6e6d920dac45","3ca6d4c5-ec01-4642-a22a-b6b636f779c0","17541dba-a598-4a0e-aa38-2ea0ea411e74","496d1c61-efa0-4401-b1cc-092445d32757","58a95307-5d92-49e5-a87b-7e304b572a0c","259f0784-ad11-4b4a-a220-0ab5a036ebc5","a0c66a71-d52c-4a15-8ad2-2d82f0b115b2"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","fcb448f9-af05-4682-a070-ffab859ebdb5","c3f331dd-6683-4c33-9460-e56bb1487f33","c5b24078-44cd-435c-b265-4e36593a226c","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","83cf99bc-d256-4e4c-a1bf-d0698ec601e9"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":[]},
    "industries/financial-services/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["4d286384-e302-4ff6-882f-f43b94ec10a6","f93af4d0-e649-48ff-9dbb-dd307c4a8c04","be7f812a-bbfe-43b4-bdc0-9cc8fccdc5ec","886d5eb0-3df8-4fe2-9d05-5d2fbdd30a05","081c2023-9a21-40d5-b522-da25a4e1a6ed"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","45c4660b-c2d9-49c6-9536-c259c7e988d6","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","874ee345-9fa7-403b-bcf3-2eaf719a89fa","1ea27058-9b4d-4a64-81f4-f56cb6b81896","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","5b69151c-e124-4dcf-b9e7-212b7320050d"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3","5ab0a0be-a65e-4648-bd51-f7a1a8850607"]},
    "industries/healthcare/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["cdb5ba62-7fe5-491a-a20b-6c463888fd36","9b46d0e7-e3a2-4971-a7cc-bf9dea41906a","ab795c9e-eac8-4374-90e7-d5792faeb33d","98cb6b1b-f560-47fb-aa72-5ef69503960e","61adc812-10ca-4d91-934e-48d15c9aec80","582cc725-7a6c-487e-8c79-d78c7f77f693","af41146b-4546-4f4e-b92e-d32ee5d8916a"],"mustHaveTagsQueryType":"OR","mustHaveTags":[],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":[]},
    "industries/industrials/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["743406e5-a0f6-49e5-918c-def0f8dfb7c5","6a5488bc-234c-426a-9f40-43e4ed2fa75c","1c7d1705-7ca8-489d-9328-d929b00bb0b6","fb1f98ed-b8d4-4a14-bb2a-c4373287b344"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","a8cf2e50-7c70-4c17-aa06-284972123380","d9ff9157-3839-4bf4-b5c3-1bb123258af5","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","c3f331dd-6683-4c33-9460-e56bb1487f33","fcb448f9-af05-4682-a070-ffab859ebdb5","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","c5b24078-44cd-435c-b265-4e36593a226c","13f7e352-920f-4dd2-9a31-f2c3314eb405","5b69151c-e124-4dcf-b9e7-212b7320050d"],"mustNotHaveTagsQueryType":"AND","mustNotHaveTags":["33f4bb5f-696d-463a-8b58-289f651b822b"]},
    "industries/infrastructure/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["df0de243-428f-47ec-b60c-801cebafe7a0","40deef57-2c53-4432-9282-e796f76dff25","e74739af-3e91-4f40-9c66-d294cb618e62","e3d5d17b-693f-42bf-8dea-4e93063c6a16","c8312621-dfeb-4d3f-81d9-d12f0fa55e0e"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","a15e9534-23d8-461d-8df6-11cef71f6441","899ef466-0f6e-4feb-965d-3882b88ac9a4","ab46faae-ee8a-40f4-90f3-a8bd97619bdf","96177f40-e988-4542-86f7-4b4533b0294d","24b3370e-8355-4d6c-89a5-f4fc7416eded","39c8cf59-8f59-4515-911e-1444de5f6adf","9637a0d4-aa07-4e26-b794-cdb1469dbb56","bd180223-13b8-4a0c-b740-db8b92f531b4","d16761fb-8877-4024-aa5e-4d0db5be24f4","fb9b154d-89ee-4562-8974-d061d0a6b807","b4c379ef-2335-4155-9d31-0624b789e0e7","f96d5958-2af2-423f-ad10-6beba7b80ca4"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/life-sciences/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["582cc725-7a6c-487e-8c79-d78c7f77f693","af41146b-4546-4f4e-b92e-d32ee5d8916a"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","ab46faae-ee8a-40f4-90f3-a8bd97619bdf","e899a9a9-ff3b-41a7-acab-d083aef59728","c3f331dd-6683-4c33-9460-e56bb1487f33"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/logistics/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["0268c217-9ef8-4a08-8631-3c7d71f48323","d9486cf6-1e7f-4b92-8ac7-5cfe52013756","a5f2641f-f246-4489-8324-547be5add42a"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","15750f40-c2b7-4634-be10-763112fad183","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","a9103a6d-6663-4a30-bf78-9927d74f5df5","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","899ef466-0f6e-4feb-965d-3882b88ac9a4","a15e9534-23d8-461d-8df6-11cef71f6441"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/packaging-and-paper/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["259f0784-ad11-4b4a-a220-0ab5a036ebc5"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/public-sector/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["c86f5dae-e73d-415c-a94a-36662c9a5237","46898d3c-ba9b-4b62-9a55-c3a61e01d2de","5728cc8a-61c3-4e84-b855-62272de5ef10"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","91cfe105-8dad-437c-8734-3d740dcdb437","15750f40-c2b7-4634-be10-763112fad183","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","1ea27058-9b4d-4a64-81f4-f56cb6b81896","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","fcb448f9-af05-4682-a070-ffab859ebdb5","c3f331dd-6683-4c33-9460-e56bb1487f33","899ef466-0f6e-4feb-965d-3882b88ac9a4","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","45c4660b-c2d9-49c6-9536-c259c7e988d6"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/retail/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["2bfc03d9-1a9b-4624-bfa1-f4991fb980b1","febe383e-849b-4273-a5a3-0ad404ef67b9","345782db-22e8-4406-b31a-1cc4d558dc86"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","ab46faae-ee8a-40f4-90f3-a8bd97619bdf","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","874ee345-9fa7-403b-bcf3-2eaf719a89fa","aaf8b700-2f9c-40e9-8037-7a90ebf4e651","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","a9103a6d-6663-4a30-bf78-9927d74f5df5"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/semiconductors/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["33f4bb5f-696d-463a-8b58-289f651b822b"],"mustHaveTagsQueryType":"OR","mustHaveTags":["91cfe105-8dad-437c-8734-3d740dcdb437","887e7cb2-191e-44d5-9ec6-de040b312a25","a9103a6d-6663-4a30-bf78-9927d74f5df5","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
    "industries/travel/our-insights": {"taxonomyQueryType":"OR","taxonomyIds":["cc7a3c3a-1d3f-4721-9975-fd94fc644dea","f0cab0f1-a728-4864-bcd6-dcbf496f306d","c9b2b85b-b301-4a47-a052-23965abb3608","9fd8034b-f0b3-4e9c-abee-5d4f515c311e","543f4100-2f8d-44f4-9420-ebaa07609f88","b03ca0a4-ae75-4022-b5f0-a18acdfcec50"],"mustHaveTagsQueryType":"OR","mustHaveTags":["0abbc29e-46ee-470b-a93b-c31f735e753a","15750f40-c2b7-4634-be10-763112fad183","21dc6bd4-9b9a-4de0-a9b8-71f68efef898","a9103a6d-6663-4a30-bf78-9927d74f5df5","a15e9534-23d8-461d-8df6-11cef71f6441","c3f331dd-6683-4c33-9460-e56bb1487f33","13f7e352-920f-4dd2-9a31-f2c3314eb405","fa4bb8d5-ea76-4dd9-9530-2c865a87d5e0","5b69151c-e124-4dcf-b9e7-212b7320050d","83cf99bc-d256-4e4c-a1bf-d0698ec601e9","899ef466-0f6e-4feb-965d-3882b88ac9a4","91cfe105-8dad-437c-8734-3d740dcdb437"],"mustNotHaveTagsQueryType":"OR","mustNotHaveTags":["a65a9603-c09a-489f-9d77-4afd71c629b3"]},
}


def _mckinsey_discover_taxonomy(list_url):
    """用 patchright 访问 McKinsey 页面，拦截 API 请求获取 taxonomyAndTags"""
    import asyncio as _aio
    import json as _j

    async def _do():
        try:
            from patchright.async_api import async_playwright
        except ImportError:
            return None

        _captured = [None]

        async def _on_req(request):
            if 'insightsgrid/articles' in request.url and request.post_data:
                try:
                    _captured[0] = _j.loads(request.post_data)
                except Exception:
                    pass

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            ctx = await browser.new_context(viewport={"width": 1440, "height": 900})
            page = await ctx.new_page()
            page.on("request", _on_req)
            try:
                await page.goto(list_url, wait_until="domcontentloaded", timeout=45000)
            except Exception:
                pass
            for _ in range(20):
                await page.wait_for_timeout(1000)
                if _captured[0]:
                    break
            await browser.close()
        return _captured[0]

    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, _do()).result(timeout=60)
        return asyncio.run(_do())
    except Exception:
        return None


def _mckinsey_parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    # 优先正则匹配，兼容 ISO datetime（如 "2026-03-11T00:00:00Z"）
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass
    for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _mckinsey_insightsgrid_fetch_themes(list_url, tax_config, cutoff, path_prefix):
    """
    与官网 themes 列表页配套的 insightsgrid 分页（站点可能同时拉博客 API + insightsgrid）。
    不按单篇旧文提前终止分页，避免漏掉后续页里的新文。
    """
    _hdrs = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.mckinsey.com",
        "Referer": list_url,
    }
    payload = {
        "limit": 100,
        "afterId": "",
        "taxonomyAndTags": tax_config,
        "excludeItems": [],
        "filters": [],
        "language": "en",
    }
    out, seen_local = [], set()
    cursor = ""
    page_num = 0
    pp_low = path_prefix.lower()
    while page_num < 40:
        payload["afterId"] = cursor
        try:
            if CURL_AVAILABLE:
                _sess = _get_curl_session("chrome124")
                _imp_mck = random.choice(["chrome124", "chrome120", "edge122", "safari17_0"])
                r = _sess.post(_MCK_API, json=payload, impersonate=_imp_mck,
                               timeout=30, headers=_hdrs)
            else:
                import json as _j
                r = http_get(_MCK_API, timeout=30, method="POST",
                             extra_headers=_hdrs,
                             body=_j.dumps(payload))
            if not r or r.status_code != 200:
                break
        except Exception:
            break
        data = r.json()
        posts = data.get("posts", [])
        has_next = data.get("hasNext", False)
        cursor = data.get("nextCursor", "")
        for post in posts:
            pub_dt = _mckinsey_parse_date(post.get("displayDate", ""))
            if pub_dt and pub_dt < cutoff:
                continue
            raw_url = post.get("url", "")
            if not raw_url:
                continue
            full_url = raw_url if raw_url.startswith("http") else \
                f"https://www.mckinsey.com{raw_url}"
            clean = full_url.split("?")[0].rstrip("/")
            try:
                _path = re.sub(r"https?://(www\.)?mckinsey\.com", "", clean, flags=re.I).lower()
            except Exception:
                _path = ""
            # 只过滤非文章页，不限制栏目路径前缀
            _clean_low2 = clean.lower()
            if any(x in _clean_low2 for x in ["/our-people/", "/contact-us/", "/about-us/",
                                                "/locations/", "/how-we-help/", ".pdf",
                                                "#", "/subscribe", "/login"]):
                continue
            if clean in seen_local:
                continue
            seen_local.add(clean)
            import html as _hm
            _t = (post.get("title") or "")[:200]
            _d = (post.get("description") or "")
            out.append({
                "url": clean,
                "title": _hm.unescape(_t),
                "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "pub_date": pub_dt,
                "summary": _hm.unescape(_d)[:500],
                "source": "mckinsey_insightsgrid",
                "_api_body_html": (post.get("body") or "").strip(),
            })
        if not has_next or not posts:
            break
        page_num += 1
    return out


def _fetch_mckinsey_themes(cutoff, list_url=None):
    """featured-insights/themes：v1/blogs/themes 分页 + 可选合并 insightsgrid（与官网列表对齐篇数）"""
    list_url = (list_url or "https://www.mckinsey.com/featured-insights/themes").split("?")[0].rstrip("/")
    _THEMES_API = "https://prd-api.mckinsey.com/v1/blogs/themes"
    _hdrs = {"Accept": "application/json", "Origin": "https://www.mckinsey.com",
             "Referer": list_url}
    articles, seen = [], set()
    print(f"\n  [McKinsey] Themes API 模式（v1/blogs/themes）")

    for pg in range(1, 100):
        try:
            if CURL_AVAILABLE:
                _sess = _get_curl_session("chrome124")
                r = _sess.get(f"{_THEMES_API}/{pg}", impersonate="chrome124",
                              timeout=20, headers=_hdrs)
            else:
                r = http_get(f"{_THEMES_API}/{pg}", timeout=20, headers=_hdrs)
            if not r or r.status_code != 200:
                break
        except Exception as e:
            print(f"    Themes API page {pg} 异常: {e}")
            break

        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        for item in results:
            dd = item.get("displaydate", "")
            pub_dt = None
            if dd:
                try:
                    pub_dt = datetime.strptime(dd[:10], "%Y-%m-%d")
                except Exception:
                    pass
            if pub_dt and pub_dt < cutoff:
                continue

            raw_url = item.get("url", "")
            if not raw_url:
                continue
            full = raw_url if raw_url.startswith("http") else \
                f"https://www.mckinsey.com{raw_url}"
            clean = full.split("?")[0].rstrip("/")
            if clean in seen:
                continue
            seen.add(clean)

            import html as _html_mod
            _raw_title = (item.get("title") or "").strip().replace("\n", " ")[:200]
            _raw_body = (item.get("body") or "").strip()
            _raw_desc = (item.get("description") or "").strip()
            articles.append({
                "url": clean,
                "title": _html_mod.unescape(_raw_title),
                "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "pub_date": pub_dt,
                "summary": _html_mod.unescape(_raw_desc)[:500],
                "source": "mckinsey_api",
                "_api_body_html": _raw_body,
            })

        if pg % 5 == 0:
            print(f"    page {pg}: 累计 {len(articles)} 篇")

    _blog_n = len(articles)
    print(f"  [McKinsey] Themes 博客 API 共 {_blog_n} 篇（cutoff >= {cutoff.strftime('%Y-%m-%d')}）")

    # 合并 insightsgrid：官网列表常同时用 blogs/themes + insightsgrid，仅博客 API 会少篇（如 42 vs 55）
    _merge = os.environ.get("NEWS_MCK_THEMES_MERGE_INSIGHTSGRID", "1").lower() not in ("0", "false", "no")
    if _merge:
        tax_key = "featured-insights/themes"
        tax_cfg = _MCK_TAXONOMY_CACHE.get(tax_key)
        if not tax_cfg:
            print("    [McKinsey] Themes: 尝试发现 insightsgrid taxonomy（headed 浏览器，约 20–60s）…")
            try:
                captured = _mckinsey_discover_taxonomy(list_url)
                if captured and captured.get("taxonomyAndTags"):
                    tax_cfg = captured["taxonomyAndTags"]
                    _MCK_TAXONOMY_CACHE[tax_key] = tax_cfg
                    print("    ✅ 已缓存 themes 页 insightsgrid taxonomy")
            except Exception as _te:
                print(f"    ⚠ insightsgrid taxonomy 发现失败，跳过合并: {_te}")
        if tax_cfg:
            try:
                _pfx = "/" + re.sub(r"https?://(www\.)?mckinsey\.com/", "", list_url, flags=re.I).strip("/") + "/"
                extra = _mckinsey_insightsgrid_fetch_themes(list_url, tax_cfg, cutoff, _pfx.lower())
                added = 0
                for row in extra:
                    u = row.get("url", "")
                    if not u or u in seen:
                        continue
                    seen.add(u)
                    articles.append(row)
                    added += 1
                print(f"    [McKinsey] insightsgrid 合并 +{added} 篇（合计 {len(articles)}）")
            except Exception as _me:
                print(f"    ⚠ insightsgrid 合并异常: {_me}")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [McKinsey] Themes 最终 {len(articles)} 篇")
    return articles


def fetch_mckinsey(list_url, months, cutoff):
    """
    McKinsey insights 列表。
    策略：prd-api.mckinsey.com/api/insightsgrid/articles POST API 分页。
    修复：
    - 不再因单篇旧文过早触发 stop_early，改为整页判断
    - 支持 NEWS_MCK_REFRESH_TAXONOMY=1 强制刷新 taxonomy
    - 增加分页诊断日志
    - 连续空页保护，防止死循环
    """
    path_key = re.sub(r'https?://(www\.)?mckinsey\.com/', '', list_url).rstrip('/')

    if "featured-insights/themes" in path_key:
        return _fetch_mckinsey_themes(cutoff, list_url)
    print(f"\n  [McKinsey] API 模式: {path_key}")

    force_refresh = os.environ.get("NEWS_MCK_REFRESH_TAXONOMY", "").lower() in ("1", "true", "yes")
    tax_config = None if force_refresh else _MCK_TAXONOMY_CACHE.get(path_key)

    if not tax_config:
        reason = "强制刷新" if force_refresh else "缓存未命中"
        print(f"    taxonomy {reason}，用 patchright 动态发现...")
        captured = _mckinsey_discover_taxonomy(list_url)
        if captured and "taxonomyAndTags" in captured:
            tax_config = captured["taxonomyAndTags"]
            _MCK_TAXONOMY_CACHE[path_key] = tax_config
            print(
                f"    ✅ 动态发现成功 "
                f"(taxonomyIds={len(tax_config.get('taxonomyIds', []))}, "
                f"mustHaveTags={len(tax_config.get('mustHaveTags', []))})"
            )
        else:
            print(f"    ❌ 动态发现失败，回退到 Playwright 滚动模式")
            def _mckinsey_filter(url):
                low = url.lower()
                if "mckinsey.com" not in low:
                    return False
                if url.lower().replace("https://www.mckinsey.com", "").count("/") < 4:
                    return False
                if not any(k in low for k in ["our-insights/", "featured-insights/"]):
                    return False
                if any(x in low for x in ["how-we-help", "contact", "subscribe", "#", ".pdf"]):
                    return False
                return True
            return _fetch_cf_site_with_playwright(
                list_url, months, cutoff, "McKinsey",
                _mckinsey_filter, max(15, months * 3))

    _hdrs = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.mckinsey.com",
        "Referer": list_url,
    }
    payload = {
        "limit": 100,
        "afterId": "",
        "taxonomyAndTags": tax_config,
        "excludeItems": [],
        "filters": [],
        "language": "en",
    }

    articles = []
    seen = set()
    cursor = ""
    page_num = 0
    consecutive_empty = 0   # 连续无新增页计数，防死循环

    while True:
        payload["afterId"] = cursor
        try:
            if CURL_AVAILABLE:
                _sess = _get_curl_session("chrome124")
                _imp_mck = random.choice(["chrome124", "chrome120", "edge122", "safari17_0"])
                r = _sess.post(_MCK_API, json=payload, impersonate=_imp_mck,
                               timeout=30, headers=_hdrs)
            else:
                import json as _j
                r = http_get(_MCK_API, timeout=30, method="POST",
                             extra_headers=_hdrs,
                             body=_j.dumps(payload))
            if not r or r.status_code != 200:
                print(f"    API page {page_num}: HTTP {r.status_code if r else 0}")
                break
        except Exception as e:
            print(f"    API page {page_num} 异常: {e}")
            break

        try:
            data = r.json()
        except Exception as e:
            print(f"    API page {page_num} JSON解析失败: {e}")
            break

        posts = data.get("posts", []) or []
        has_next = data.get("hasNext", False)
        next_cursor = data.get("nextCursor", "") or ""

        # 诊断日志：每页都打印
        _cursor_preview = (next_cursor[:24] + "...") if next_cursor else ""
        print(f"    page {page_num}: posts={len(posts)} has_next={has_next} cursor={_cursor_preview}")

        page_added = 0
        page_oldest_dt = None
        page_all_before_cutoff = True

        import html as _html_mod2
        for post in posts:
            pub_dt = _mckinsey_parse_date(post.get("displayDate", ""))
            if pub_dt:
                page_oldest_dt = pub_dt if page_oldest_dt is None else min(page_oldest_dt, pub_dt)

            # 关键修复：只跳过这篇文章，不设 stop_early 打断整轮循环
            if pub_dt and pub_dt < cutoff:
                continue

            page_all_before_cutoff = False  # 本页至少有一篇在范围内

            raw_url = post.get("url", "")
            if not raw_url:
                continue
            full_url = raw_url if raw_url.startswith("http") else \
                f"https://www.mckinsey.com{raw_url}"
            clean = full_url.split("?")[0].rstrip("/")

            _clean_low = clean.lower()
            if any(x in _clean_low for x in ["/our-people/", "/contact-us/", "/about-us/",
                                               "/locations/", "/how-we-help/", ".pdf",
                                               "#", "?", "/subscribe", "/login"]):
                continue
            if clean in seen:
                continue
            seen.add(clean)

            articles.append({
                "url": clean,
                "title": _html_mod2.unescape((post.get("title") or "")[:200]),
                "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "pub_date": pub_dt,
                "summary": _html_mod2.unescape((post.get("description") or ""))[:500],
                "source": "mckinsey_api",
                "_api_body_html": (post.get("body") or "").strip(),
            })
            page_added += 1

        _oldest_str = page_oldest_dt.strftime('%Y-%m-%d') if page_oldest_dt else "-"
        print(f"    → page {page_num} 新增 {page_added} 篇，累计 {len(articles)} 篇，oldest={_oldest_str}")

        # 连续空页保护
        if page_added == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0
        if consecutive_empty >= 2:
            print(f"    → 连续 {consecutive_empty} 页无新增，停止")
            break

        # 整页都早于 cutoff 才停止（不是单篇）
        if page_oldest_dt and page_oldest_dt < cutoff and page_all_before_cutoff:
            print("    → 当前页全部早于 cutoff，停止分页")
            break

        if not has_next or not posts:
            break

        # cursor 未变化保护
        if next_cursor and next_cursor == cursor:
            print("    → nextCursor 未变化，停止分页")
            break

        cursor = next_cursor
        page_num += 1

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [McKinsey] 共 {len(articles)} 篇（cutoff >= {cutoff.strftime('%Y-%m-%d')}）")
    return articles


def fetch_kearney(list_url, months, cutoff):
    def _kearney_filter(url):
        low = url.lower()
        if "kearney.com" not in low: return False
        path = url.replace("https://www.kearney.com","").replace("http://www.kearney.com","")
        # 必须有 /article/ 路径段才是文章（过滤导航页、分类页）
        if "/article/" not in path.lower() and "/insight/" not in path.lower():
            return False
        path_depth = path.count("/")
        if path_depth < 3: return False
        if any(x in low for x in ["contact","career","about","privacy","cookie",
                                    "subscribe","#",".pdf","login","register",
                                    "search","sitemap","how-we-help"]): return False
        return True
    return _fetch_cf_site_with_playwright(list_url, months, cutoff, "Kearney",
                                          _kearney_filter, max(20, months*4))



# ════════════════════════════════════════════════════
# Oliver Wyman 专属：XHR 拦截 + Load More 循环
#
# 背景：
#   oliverwyman.com/our-expertise/insights.html 是 React/AEM SPA，
#   DOM 内容完全由 JS 动态注入，静态 http_get 只能拿到骨架 HTML。
#   页面首屏渲染 ~19 篇，其余通过点击「Load More」触发 XHR 加载。
#
# 策略：
#   Phase-1  先用 patchright 拦截 XHR，抓到后台 API 端点 + 参数格式
#   Phase-2  若找到 API → 直接翻页调用（速度快 10x）
#            若未找到 → 回退到 Playwright 模拟点击 Load More
#   两种路径都做了截图 / 日期过滤 / 去重处理。
#
# 截图问题修复：
#   单篇截图走 take_screenshot()，该函数对 CF 域名（oliverwyman.com
#   在 _CF_DOMAINS 列表中）已启用 headless=False + CF 等待逻辑；
#   之前截图全白是因为走了通用 fetch_generic（无 CF 等待 + 无 networkidle）。
# ════════════════════════════════════════════════════
async def fetch_oliverwyman(list_url: str, months: int, cutoff) -> list:
    """
    Oliver Wyman insights 列表页全量抓取  v2.0
    ═══════════════════════════════════════════════════════════════════
    问题根因（已确认）：
      • OW insights 页面是 AEM + React SPA，DOM 内容 100% JS 渲染
      • 列表通过分页（Previous/Next）加载，不是 Load More 无限滚动
      • 每页约 10 篇，108 篇 → 约 11 页
      • 静态 http_get / 截图过早 → 只拿到骨架 HTML，内容为空

    三重抓取策略（按优先级）：
      策略1  XHR 拦截 + 分页自动翻页（最快，找到 API 后直接翻）
      策略2  Playwright DOM 翻页（点击 Next 按钮逐页提取链接）
      策略3  AEM 常见路径探测（/bin/querybuilder.json 等静态备用）

    截图等待修复：
      单篇截图走 take_screenshot() → 已有 CF headless=False 逻辑；
      此处只负责列表收集，截图问题在 take_screenshot 里另行修复。
    """
    import json as _json
    from urllib.parse import urlencode, urlparse as _up, urlunparse

    BASE = "https://www.oliverwyman.com"

    # ── OW 文章 URL 过滤器 ─────────────────────────────────────────
    def _is_ow_article(u: str) -> bool:
        low = u.lower()
        if "oliverwyman.com" not in low:
            return False
        # 文章固定路径格式: /our-expertise/insights/YYYY/mon/slug.html
        if "/our-expertise/insights/" not in low:
            return False
        tail = low.rstrip("/").split("/our-expertise/insights/")[-1]
        # 必须有年份段（4位数字）
        if not re.search(r'^\d{4}/', tail):
            return False
        for skip in ("privacy", "cookie", "contact", "career",
                     "subscribe", "sitemap", "login", "search", "#"):
            if skip in low:
                return False
        return True

    # ── 从 DOM 节点提取文章信息（href + title + date） ─────────────
    def _extract_from_dom_items(items: list, seen: set) -> list:
        out = []
        for it in items:
            href  = (it.get("href") or "").strip()
            text  = (it.get("text") or "").strip()
            dt_raw = (it.get("datetime") or "").strip()
            if not href:
                continue
            full = BASE + href if href.startswith("/") else href
            full = full.split("?")[0].rstrip("/")
            if not _is_ow_article(full) or full in seen:
                continue
            seen.add(full)
            # 日期：先用 DOM 的 datetime，再从 URL 路径推断年/月
            pub_dt   = parse_date(dt_raw) if dt_raw else None
            date_str = pub_dt.strftime("%Y-%m-%d") if pub_dt else ""
            if not pub_dt:
                m = re.search(r'/(\d{4})/([a-z]{3})/', full.lower())
                if m:
                    _mon = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                            "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
                    try:
                        pub_dt   = datetime(int(m.group(1)), _mon.get(m.group(2), 1), 1)
                        date_str = pub_dt.strftime("%Y-%m-01")
                    except Exception:
                        pass
            if pub_dt and pub_dt < cutoff:
                continue
            title = text or full.rstrip("/").split("/")[-1].replace("-", " ").title()
            out.append({
                "url": full, "title": title[:200],
                "date": date_str, "pub_date": pub_dt,
                "summary": "", "source": "oliverwyman",
            })
        return out

    articles: list = []
    seen:     set  = set()
    api_endpoint:  str  = ""
    api_base_params: dict = {}

    try:
        from patchright.async_api import async_playwright as _apw
    except ImportError:
        from playwright.async_api import async_playwright as _apw

    # ═══════════════════════════════════════════════════════════════
    # 策略1 + 策略2：Playwright 打开列表页
    #   • 同时拦截 XHR（找 API）
    #   • 同时自动点击分页 Next 按钮（DOM 翻页）
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  [Oliver Wyman] patchright 启动（headless=False，等待 CF 验证）...")

    captured_api: list = []   # 拦截到的 (url, json_data) 列表

    async def _run_playwright() -> None:
        nonlocal api_endpoint, api_base_params

        async with _apw() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=["--no-sandbox",
                      "--disable-blink-features=AutomationControlled",
                      "--start-maximized"],
                slow_mo=40,
            )
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )

            # ── XHR 拦截器 ─────────────────────────────────────────
            async def _on_resp(resp):
                try:
                    rurl  = resp.url
                    if "oliverwyman.com" not in rurl:
                        return
                    ct = resp.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    skip = ("analytics","gtm","adobe","segment","fonts",
                            "typekit","telemetry","track","collect")
                    if any(k in rurl.lower() for k in skip):
                        return
                    body  = await resp.body()
                    if len(body) < 300:
                        return
                    data  = _json.loads(body)
                    # 判断是否含文章列表结构
                    has_list = False
                    if isinstance(data, dict):
                        for k in ("results","items","hits","data","articles",
                                  "content","nodes","pages","response"):
                            v = data.get(k)
                            if isinstance(v, list) and v:
                                s = v[0]
                                if isinstance(s, dict) and any(
                                    f in s for f in
                                    ("url","path","href","title","headline",
                                     "jcrPath","pagePath","link")
                                ):
                                    has_list = True
                                    break
                            if isinstance(v, dict):
                                inner = (v.get("hits") or v.get("items")
                                         or v.get("results") or [])
                                if isinstance(inner, list) and inner:
                                    has_list = True
                                    break
                    if not has_list:
                        return
                    captured_api.append((rurl, data))
                    print(f"         📡 XHR 拦截: {rurl[:110]}")
                    if not api_endpoint:
                        nonlocal api_base_params
                        api_endpoint_tmp = rurl.split("?")[0]
                        # 解析现有 query 参数作为翻页基准
                        from urllib.parse import parse_qs as _pqs
                        _pu  = _up(rurl)
                        _qp  = _pqs(_pu.query, keep_blank_values=True)
                        api_base_params  = {k: v[0] for k, v in _qp.items()}
                        # 用 nonlocal 更新外层变量
                        import ctypes as _ct
                        # 直接赋值外层 nonlocal（Python 允许）
                        pass
                        # 注：nonlocal 赋值需在函数体内，通过列表传递
                        captured_api.insert(0, ("__endpoint__", api_endpoint_tmp))
                except Exception:
                    pass

            page = await ctx.new_page()
            page.on("response", _on_resp)

            # ── 打开列表页 ─────────────────────────────────────────
            try:
                await page.goto(list_url, wait_until="domcontentloaded",
                                timeout=60000)
            except Exception:
                pass

            # ── 等待 CF 验证通过（最多 30 秒）─────────────────────
            print("         等待 CF 验证通过...")
            for _i in range(30):
                await page.wait_for_timeout(1000)
                _t = await page.title()
                _c = await page.content()
                _cf = any(x in (_t + _c) for x in [
                    "Just a moment", "Performing security verification",
                    "Checking your browser", "cf-challenge",
                    "Enable JavaScript", "DDoS protection",
                ])
                if not _cf:
                    print(f"         ✓ CF 验证通过（{_i+1}s）")
                    break
            else:
                print("         ⚠ CF 验证超时，尝试继续...")

            # ── 等待文章列表渲染（关键！）─────────────────────────
            # 等待至少 1 个文章链接出现在 DOM 里
            _article_sel = "a[href*='/our-expertise/insights/']"
            try:
                await page.wait_for_selector(_article_sel, timeout=20000)
                print("         ✓ 文章列表已渲染")
            except Exception:
                print("         ⚠ 文章列表未出现，继续等待 5 秒...")
                await page.wait_for_timeout(5000)

            # ── 关闭 Cookie 弹窗 ───────────────────────────────────
            for ck in ["#onetrust-accept-btn-handler",
                       "button:has-text('Accept All')",
                       "button:has-text('Accept all')",
                       "button:has-text('Accept')"]:
                try:
                    b = page.locator(ck).first
                    if await b.is_visible(timeout=1500):
                        await b.click()
                        await page.wait_for_timeout(1000)
                        print(f"         ✓ Cookie 弹窗关闭")
                        break
                except Exception:
                    pass

            # ── 等待 networkidle（等 XHR 完成）────────────────────
            try:
                await page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                await page.wait_for_timeout(3000)

            # ── JS 提取当前页面所有文章链接（含日期 datetime）──────
            async def _extract_page_links():
                return await page.evaluate("""() => {
                    const out = [];
                    const seen = new Set();
                    // 先找带 datetime 的 <time> 元素附近的链接
                    document.querySelectorAll('a[href]').forEach(a => {
                        const href = a.getAttribute('href') || '';
                        if (!href.includes('/our-expertise/insights/')) return;
                        if (href.endsWith('/insights.html') ||
                            href.endsWith('/insights')) return;
                        if (seen.has(href)) return;
                        seen.add(href);
                        // 找最近的 <time> 或含日期 class 的元素
                        let dt = '';
                        let el = a;
                        for (let i = 0; i < 8 && el; i++) {
                            const t = el.querySelector('time');
                            if (t) {
                                dt = t.getAttribute('datetime') ||
                                     t.textContent || '';
                                break;
                            }
                            // class 含 date/Date/publish 的元素
                            const d = el.querySelector(
                                '[class*="date"],[class*="Date"],' +
                                '[class*="publish"],[class*="time"]'
                            );
                            if (d) {
                                dt = d.getAttribute('datetime') ||
                                     d.textContent || '';
                                break;
                            }
                            el = el.parentElement;
                        }
                        // 标题：优先 a 自身文字，否则找兄弟/父级 h2/h3
                        let title = (a.innerText || '').trim();
                        if (!title || title.length < 5) {
                            let p = a.parentElement;
                            for (let i = 0; i < 6 && p; i++) {
                                const h = p.querySelector('h2,h3,h4,[class*="title"],[class*="Title"]');
                                if (h && h.innerText.trim().length > 5) {
                                    title = h.innerText.trim();
                                    break;
                                }
                                p = p.parentElement;
                            }
                        }
                        out.push({
                            href,
                            text: title.replace(/\\s+/g, ' ').slice(0, 200),
                            datetime: dt.trim().slice(0, 30),
                        });
                    });
                    return out;
                }""")

            # ── 逐页翻页，提取全部文章 ───────────────────────────
            # OW insights 用分页（Next 按钮），不是无限滚动
            MAX_PAGES = max(15, months * 4)
            page_num  = 1
            consecutive_no_new = 0

            NEXT_SELECTORS = [
                "button:has-text('Next')",
                "a:has-text('Next')",
                "[aria-label='Next page']",
                "[aria-label='Next']",
                ".pagination__next",
                "[class*='pagination'] [class*='next']",
                "[class*='pager'] [class*='next']",
                "nav[aria-label*='pagination'] a[rel='next']",
                # OW 具体类名（从 HTML 骨架可见有 Previous/Next 锚点）
                "a:has-text('Next page')",
            ]

            for _pg in range(MAX_PAGES):
                # 提取当前页链接
                items = await _extract_page_links()
                new_arts = _extract_from_dom_items(items, seen)
                articles.extend(new_arts)

                print(f"         页{page_num}: 提取 {len(items)} 链接 → "
                      f"新增 {len(new_arts)} 篇（累计 {len(articles)} 篇）")

                # 截止日期检查
                if new_arts:
                    consecutive_no_new = 0
                    oldest = min(
                        (a["pub_date"] for a in new_arts if a.get("pub_date")),
                        default=None
                    )
                    if oldest and oldest < cutoff:
                        print(f"         已达 cutoff（{oldest.strftime('%Y-%m-%d')}），停止翻页")
                        break
                else:
                    consecutive_no_new += 1
                    if consecutive_no_new >= 2:
                        print(f"         连续 2 页无新内容，停止翻页")
                        break

                # 找 Next 按钮
                next_btn = None
                for _sel in NEXT_SELECTORS:
                    try:
                        _b = page.locator(_sel).first
                        if await _b.is_visible(timeout=800):
                            # 确认不是 disabled
                            _disabled = await _b.get_attribute("disabled")
                            _aria_dis = await _b.get_attribute("aria-disabled")
                            _cls      = (await _b.get_attribute("class")) or ""
                            if (_disabled is not None or
                                    _aria_dis == "true" or
                                    "disabled" in _cls.lower()):
                                print(f"         Next 按钮已禁用，到达最后一页")
                                next_btn = None
                                break
                            next_btn = _b
                            break
                    except Exception:
                        pass

                if next_btn is None:
                    print(f"         未找到可用 Next 按钮，翻页结束")
                    break

                # 点击 Next
                try:
                    await next_btn.scroll_into_view_if_needed()
                    await next_btn.click()
                    page_num += 1
                    # 等待新内容加载：链接数量变化 或 networkidle
                    _prev_count = len(seen)
                    for _wait in range(15):
                        await page.wait_for_timeout(1000)
                        try:
                            await page.wait_for_load_state("networkidle",
                                                           timeout=3000)
                            break
                        except Exception:
                            pass
                        _new_links = await page.evaluate("""() =>
                            document.querySelectorAll(
                                "a[href*='/our-expertise/insights/']"
                            ).length
                        """)
                        if _new_links != _prev_count:
                            await page.wait_for_timeout(1500)
                            break
                    else:
                        await page.wait_for_timeout(3000)
                except Exception as _ce:
                    print(f"         ⚠ 点击 Next 失败: {_ce}")
                    break

            # ── 解析拦截到的 XHR 数据（作为补充/验证）──────────────
            _api_ep = ""
            for entry in captured_api:
                if entry[0] == "__endpoint__":
                    _api_ep = entry[1]
                    continue
            if captured_api:
                print(f"\n         XHR 拦截共 {len(captured_api)} 次，解析补充数据...")
                for _rurl, _data in captured_api:
                    if _rurl == "__endpoint__":
                        continue
                    for _k in ("results","items","hits","data","articles",
                               "content","nodes","pages","response"):
                        _v = _data.get(_k)
                        if isinstance(_v, list) and _v:
                            for _item in _v:
                                if not isinstance(_item, dict):
                                    continue
                                _u = (_item.get("url") or _item.get("path") or
                                      _item.get("href") or _item.get("link") or
                                      _item.get("pagePath") or "").strip()
                                if not _u:
                                    continue
                                if not _u.startswith("http"):
                                    _u = BASE + _u
                                _u = _u.split("?")[0].rstrip("/")
                                if not _is_ow_article(_u) or _u in seen:
                                    continue
                                seen.add(_u)
                                _title = (_item.get("title") or
                                          _item.get("headline") or "").strip()
                                _dr = (_item.get("date") or
                                       _item.get("publishedDate") or
                                       _item.get("publicationDate") or "")
                                _pdt = parse_date(str(_dr)) if _dr else None
                                _ds  = _pdt.strftime("%Y-%m-%d") if _pdt else ""
                                if _pdt and _pdt < cutoff:
                                    continue
                                articles.append({
                                    "url": _u, "title": _title[:200] or _u.split("/")[-1],
                                    "date": _ds, "pub_date": _pdt,
                                    "summary": "", "source": "oliverwyman",
                                })
                            break

            await browser.close()
            # 把 api_ep 传出
            if _api_ep:
                captured_api.append(("__ep_result__", _api_ep))

    await _run_playwright()

    # ── 如果 Playwright DOM 翻页拿到的太少（< 10 篇），尝试 AEM querybuilder ──
    # OW 用 AEM，常见 servlet 端点：
    #   /bin/querybuilder.json
    #   /bin/oliverwyman/insights.json
    #   /content/oliverwyman/us/en/our-expertise/insights.infinity-list.json
    if len(articles) < 10:
        print(f"\n  [Oliver Wyman] DOM 方式只拿到 {len(articles)} 篇，尝试 AEM 探测...")
        _aem_endpoints = [
            (f"{BASE}/bin/querybuilder.json?"
             "type=cq%3APage"
             "&path=%2Fcontent%2Foliverwyman%2Fus%2Fen%2Four-expertise%2Finsights"
             "&p.limit=100&p.offset=0"
             "&orderby=jcr%3Acontent%2FcqLastModified&orderby.sort=desc"
             "&property=jcr%3Acontent%2FcqLastModified"
             "&property.operation=exists", "querybuilder"),
            (f"{BASE}/content/oliverwyman/us/en/our-expertise/insights"
             ".infinity-list.json?offset=0&limit=100", "infinity-list"),
            (f"{BASE}/bin/oliverwyman/insights.json?offset=0&limit=100",
             "insights-bin"),
        ]
        for _ep, _name in _aem_endpoints:
            _r = http_get(_ep, timeout=15)
            if not _r or _r.status_code != 200:
                print(f"    ⚠ {_name}: HTTP {_r.status_code if _r else 0}")
                continue
            try:
                _d = _r.json()
                print(f"    ✓ {_name}: {str(_d)[:120]}")
            except Exception:
                print(f"    ✓ {_name}: 返回非 JSON")
            time.sleep(0.5)

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"\n  [Oliver Wyman] 共收集 {len(articles)} 篇"
          f"（cutoff={cutoff.strftime('%Y-%m-%d')}）")
    return articles



# ════════════════════════════════════════════════════
# Morgan Stanley 专属：curl_cffi 直接抓取
# patchright 会报 ERR_HTTP2_PROTOCOL_ERROR，用 curl_cffi 绕过
# 文章列表通过 /insights 页面分页，文章链接格式 /insights/articles/xxx
# ════════════════════════════════════════════════════
def fetch_morganstanley(list_url, months, cutoff):
    """
    Morgan Stanley /insights 文章列表。
    策略：AEM JSON API（insights-automation.json），只取 article 类型，
    排除 podcast / video。
    """
    articles = []
    seen = set()

    _API = "https://www.morganstanley.com/insights.insights-automation.json"
    print(f"  [Morgan Stanley] AEM JSON API 模式")

    def _parse_ms_date(s):
        if not s:
            return None
        for fmt in ("%b %d, %Y", "%Y-%m-%d", "%B %d, %Y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return parse_date(s)

    try:
        _hdrs = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.morganstanley.com/insights",
        }
        if CURL_AVAILABLE:
            sess = _req.Session()
            r = sess.get(_API, params={"search": "recirculationgrid_co"},
                         impersonate="chrome124", timeout=30, headers=_hdrs)
        else:
            r = http_get(_API + "?search=recirculationgrid_co",
                         timeout=30, extra_headers=_hdrs)

        if not r or r.status_code != 200:
            print(f"  ⚠ API HTTP {r.status_code if r else 0}")
            return articles

        data = r.json()
        print(f"  API 返回 {len(data)} 条")

        n_skip_type = 0
        n_skip_date = 0

        for item in data:
            media = item.get("mediaType", "")
            if "article" not in media.lower():
                n_skip_type += 1
                continue

            pub_dt = _parse_ms_date(item.get("publishedAt", ""))
            if pub_dt and pub_dt < cutoff:
                n_skip_date += 1
                continue

            page_url = item.get("pageUrl", "")
            if not page_url:
                aem_id = item.get("id", "")
                if aem_id:
                    page_url = "https://www.morganstanley.com" + aem_id.replace(
                        "/content/msdotcom/en", "")
            clean = page_url.split("?")[0].rstrip("/")
            if not clean or clean in seen:
                continue
            seen.add(clean)

            articles.append({
                "url": clean,
                "title": (item.get("title") or "")[:200],
                "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                "pub_date": pub_dt,
                "summary": (item.get("description") or "")[:300],
                "source": "morganstanley_api",
            })

        print(f"  跳过: 非 article {n_skip_type}, 早于 cutoff {n_skip_date}")

    except Exception as e:
        print(f"  ⚠ API 获取失败: {e}")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [Morgan Stanley] 共 {len(articles)} 篇 article"
          f"（cutoff >= {cutoff.strftime('%Y-%m-%d')}）")
    return articles


# ════════════════════════════════════════════════════
# BCG Publications 专属：Fragment POST API 分页
# （CF 保护但 API 可直接调用）
# ════════════════════════════════════════════════════
def fetch_bcg(list_url, cutoff):
    """
    BCG /publications 文章列表。
    策略：页面内嵌 <filtering-insights> Web Component，
    通过 POST /publications?_fragmentId=... 分页获取 HTML 卡片，
    每页 8 条，逐页翻页直到日期早于 cutoff 或连续 3 页无新结果。
    """
    articles = []
    seen = set()

    print(f"  [BCG] Fragment POST API 模式")

    def _parse_bcg_date(s):
        if not s:
            return None
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y"):
            try:
                return datetime.strptime(s.strip(), fmt)
            except Exception:
                continue
        return parse_date(s)

    try:
        if CURL_AVAILABLE:
            sess = _req.Session()
            r0 = sess.get("https://www.bcg.com/publications",
                          impersonate="chrome124", timeout=20)
        else:
            r0 = http_get("https://www.bcg.com/publications", timeout=20)

        if not r0 or r0.status_code != 200:
            print(f"  ⚠ 主页 HTTP {r0.status_code if r0 else 0}")
            return articles

        fi_match = re.search(
            r'data-fragmenturi="([^"]+)"', r0.text, re.IGNORECASE)
        if not fi_match:
            print("  ⚠ 未找到 data-fragmentUri")
            return articles

        from html import unescape as _html_unescape
        api_url = "https://www.bcg.com" + _html_unescape(fi_match.group(1))
        print(f"  API: {api_url[:80]}...")

        _hdrs = {
            "Content-Type": "application/json",
            "Referer": "https://www.bcg.com/publications",
        }
        empty_streak = 0
        all_before = 0

        for pg in range(80):
            if CURL_AVAILABLE:
                r = sess.post(api_url, impersonate="chrome124", timeout=20,
                              headers=_hdrs,
                              json={"Topics": [], "pg": str(pg)})
            else:
                import json as _json
                r = http_get(api_url, timeout=20, method="POST",
                             extra_headers=_hdrs,
                             body=_json.dumps({"Topics": [], "pg": str(pg)}))

            if not r or r.status_code != 200:
                break

            try:
                data = r.json()
            except Exception:
                break

            items = data.get("items", [])
            if not items:
                break

            new_in_page = 0
            before_in_page = 0

            for item_html in items:
                soup = BeautifulSoup(item_html, "html.parser")
                text = soup.get_text(separator=" | ", strip=True)

                pub_links = soup.select('a[href*="/publications/"]')
                if not pub_links:
                    continue

                href = pub_links[0].get("href", "")
                if not href or href in seen:
                    continue

                date_m = re.search(
                    r"((?:January|February|March|April|May|June|July|"
                    r"August|September|October|November|December)"
                    r"\s+\d{1,2},?\s+\d{4})", text)
                date_str = date_m.group(1) if date_m else ""
                pub_dt = _parse_bcg_date(date_str)

                if pub_dt and pub_dt < cutoff:
                    before_in_page += 1
                    continue

                seen.add(href)
                new_in_page += 1

                title = ""
                summary = ""
                for a in pub_links:
                    t = a.get_text(strip=True)
                    if t and t != "Learn More" and len(t) > 5:
                        title = t
                        break

                parts = [p.strip() for p in text.split("|")]
                for p in parts:
                    if len(p) > 40:
                        summary = p
                        break

                if not href.startswith("http"):
                    href = "https://www.bcg.com" + href

                articles.append({
                    "url": href.split("?")[0].rstrip("/"),
                    "title": title[:200],
                    "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                    "pub_date": pub_dt,
                    "summary": summary[:300],
                    "source": "bcg_api",
                })

            if new_in_page == 0:
                empty_streak += 1
                if empty_streak >= 3:
                    break
            else:
                empty_streak = 0

            if pg % 10 == 0:
                print(f"    page {pg}: +{new_in_page} 篇"
                      f"（累计 {len(articles)}）")

    except Exception as e:
        print(f"  ⚠ BCG API 获取失败: {e}")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [BCG] 共 {len(articles)} 篇"
          f"（cutoff >= {cutoff.strftime('%Y-%m-%d')}）")
    return articles


# ════════════════════════════════════════════════════
# Bain & Company Insights 专属：Wayback CDX API 差分发现
# （CF Turnstile 无法绕过，用 CDX 时间差找新发布文章）
# ════════════════════════════════════════════════════
def fetch_bain_insights(list_url, cutoff):
    """
    Bain.com 全站 CF Turnstile，无法直接访问页面或 API。
    策略：Wayback Machine CDX API 差分法 ——
      1. 查询 cutoff 之前所有已知的 /insights/* URL 集合（old_urls）
      2. 查询 cutoff 之后所有 /insights/* URL 集合（recent_urls）
      3. 差集 = 新发表的文章
      4. 按 slug 模式过滤掉 snap-chart / podcast / video / webinar / infographic 等
    """
    import time as _time

    CDX_BASE = "https://web.archive.org/cdx/search/cdx"
    cutoff_ymd = cutoff.strftime("%Y%m%d")

    _SKIP_SUFFIXES = ("-snap-chart", "-infographic", "-podcast", "-video",
                      "-webinar", "-interactive")
    _SKIP_PREFIXES = ("btb-", "ceo-sessions-", "recruiting-", "why-bain-",
                      "inside-bain-")
    _SKIP_CONTAINS = ("%5c", "%5C", "-external-direct-link")
    _SKIP_EXACT = ("what-is-artificial-intelligence", "what-is-machine-learning",
                   "what-is-responsible-ai", "what-is-artificial-general-intelligence-agi")

    def _norm_url(raw):
        """归一化 CDX 返回的 URL"""
        u = raw.split("?")[0].rstrip("/")
        if not u.startswith("http"):
            u = "https://" + u
        u = re.sub(r"https?://(www\.)?bain\.com", "https://www.bain.com", u)
        return u.lower()

    def _extract_slug(url):
        """从 /insights/SLUG 中取 slug"""
        if "/insights/" not in url:
            return ""
        slug = url.split("/insights/")[-1].rstrip("/")
        if "/" in slug or len(slug) < 10:
            return ""
        return slug

    def _is_article_slug(slug):
        if not slug:
            return False
        for s in _SKIP_SUFFIXES:
            if slug.endswith(s):
                return False
        for p in _SKIP_PREFIXES:
            if slug.startswith(p):
                return False
        for c in _SKIP_CONTAINS:
            if c in slug:
                return False
        if slug in _SKIP_EXACT:
            return False
        if ")" in slug or "\\" in slug:
            return False
        return True

    def _cdx_fetch_urls(params, label="", max_retries=3):
        """调用 CDX API，返回 {norm_url: first_timestamp}，含重试逻辑"""
        result = {}
        for attempt in range(max_retries):
            try:
                if CURL_AVAILABLE:
                    from curl_cffi import requests as _cffi
                    sess = _cffi.Session()
                    r = sess.get(CDX_BASE, params=params, impersonate="chrome124",
                                 timeout=120)
                else:
                    r = http_get(CDX_BASE + "?" + "&".join(
                        f"{k}={v}" for k, v in params.items()), timeout=120)
                if r and r.status_code == 200:
                    data = r.json()
                    for row in data[1:]:
                        url = _norm_url(row[0])
                        ts = row[1] if len(row) > 1 else ""
                        slug = _extract_slug(url)
                        if not slug:
                            continue
                        if url not in result or (ts and ts < result[url]):
                            result[url] = ts
                    print(f"    CDX {label}: {len(data)-1} rows → {len(result)} unique")
                    return result
                elif r and r.status_code in (502, 503, 504):
                    wait = 10 * (attempt + 1)
                    print(f"    CDX {label}: HTTP {r.status_code}，{wait}s 后重试 "
                          f"({attempt+1}/{max_retries})")
                    _time.sleep(wait)
                    continue
                else:
                    print(f"    CDX {label}: HTTP {r.status_code if r else 0}")
                    return result
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 10 * (attempt + 1)
                    print(f"    CDX {label} 异常: {e}，{wait}s 后重试")
                    _time.sleep(wait)
                else:
                    print(f"    CDX {label} 失败: {e}")
        return result

    print(f"  [Bain] CDX 差分法（cutoff = {cutoff.strftime('%Y-%m-%d')}）")

    # ── 1. 获取 cutoff 之前存在的所有 insight URL ──
    print("  [Bain] 步骤1: 获取 cutoff 之前的旧 URL 集合...")
    old_urls = set()
    for from_d, to_d, lbl in [("19900101", "20240101", "~2024"),
                               ("20240101", cutoff_ymd, f"2024~{cutoff_ymd}")]:
        batch = _cdx_fetch_urls({
            "url": "bain.com/insights/*",
            "output": "json",
            "fl": "original",
            "filter": "statuscode:200",
            "from": from_d, "to": to_d,
            "collapse": "urlkey",
            "limit": "50000",
        }, label=lbl)
        old_urls.update(batch.keys())
        _time.sleep(1)
    print(f"    旧 URL 总计: {len(old_urls)}")

    # ── 2. 获取 cutoff 之后出现的所有 insight URL ──
    print("  [Bain] 步骤2: 获取 cutoff 之后的新 URL 集合...")
    recent_map = _cdx_fetch_urls({
        "url": "bain.com/insights/*",
        "output": "json",
        "fl": "original,timestamp",
        "filter": "statuscode:200",
        "from": cutoff_ymd,
        "collapse": "urlkey",
        "limit": "50000",
    }, label=f"from {cutoff_ymd}")

    # ── 3. 差集 = 新文章 ──
    new_urls = {u: ts for u, ts in recent_map.items() if u not in old_urls}
    print(f"  [Bain] 差集（新出现 URL）: {len(new_urls)}")

    # ── 4. 过滤为纯 article ──
    articles = []
    seen = set()
    n_skip = 0
    for url, ts in new_urls.items():
        slug = _extract_slug(url)
        if not _is_article_slug(slug):
            n_skip += 1
            continue
        canon = url.split("?")[0].rstrip("/")
        if canon in seen:
            continue
        seen.add(canon)

        pub_dt = None
        if ts and len(ts) >= 8:
            try:
                pub_dt = datetime.strptime(ts[:8], "%Y%m%d")
            except Exception:
                pass

        title = slug.replace("-", " ").strip().title()

        articles.append({
            "url": re.sub(r"https?://(www\.)?bain\.com",
                          "https://www.bain.com", canon),
            "title": title[:200],
            "date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
            "pub_date": pub_dt,
            "summary": "",
            "source": "bain_cdx",
        })
    print(f"    过滤掉非 article: {n_skip}，保留: {len(articles)}")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"\n  [Bain] 最终文章数: {len(articles)} 篇"
          f"（cutoff >= {cutoff.strftime('%Y-%m-%d')}）")

    return articles


# ════════════════════════════════════════════════════
# Oliver Wyman Insights 专属：AEM 聚合 JSON 接口
# ════════════════════════════════════════════════════
def fetch_oliverwyman_insights(list_url, cutoff):
    """
    Oliver Wyman Insights 列表页通过 AEM content_aggregator 加载卡片。
    前端 clientlib 用 `{data-aggregator-name}.ow.{batch}.json` 拉取；
    `.ow.99999.json` = Load All，一次返回全部条目（含 path/date/title）。
    """
    from urllib.parse import quote as _q

    u0 = urlparse(list_url)
    netloc = u0.netloc or "www.oliverwyman.com"
    scheme = u0.scheme or "https"

    r = http_get(list_url, timeout=45, retries=3)
    html = r.text if (r and r.status_code == 200 and r.text) else ""
    if not html:
        print("  [Oliver Wyman] 无法获取列表页 HTML")
        return []

    names = re.findall(r'data-aggregator-name="([^"]+)"', html, flags=re.IGNORECASE)
    raw = ""
    for n in names:
        if "content_aggregator" in n:
            raw = n.strip()
            break
    if not raw and names:
        raw = names[0].strip()
    if not raw.startswith("/content/"):
        print("  [Oliver Wyman] 未找到有效的 data-aggregator-name")
        return []

    parts = [p for p in raw.split("/") if p]
    enc_path = "/" + "/".join(_q(p, safe="") for p in parts)
    batch = (os.environ.get("NEWS_SCRAPER_OW_BATCH_SUFFIX") or "99999").strip() or "99999"
    endpoint = f"{scheme}://{netloc}{enc_path}.ow.{batch}.json"

    print(f"  [Oliver Wyman] 聚合 JSON: ...{enc_path[-72:]}.ow.{batch}.json")

    # 必须用标准 requests（非 curl_cffi）：JSON 响应可达数 MB，curl_cffi 可能截断
    import requests as _std_requests
    try:
        jr = _std_requests.get(
            endpoint,
            headers={
                "User-Agent": make_headers().get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
        )
    except Exception as e:
        print(f"  [Oliver Wyman] 请求失败: {e}")
        return []

    if not (jr and jr.status_code == 200 and getattr(jr, "text", None)):
        print(f"  [Oliver Wyman] HTTP {getattr(jr, 'status_code', None)}")
        return []

    print(f"  [Oliver Wyman] 响应大小: {len(jr.text)} 字节")

    try:
        data = jr.json()
    except Exception:
        print("  [Oliver Wyman] JSON 解析失败")
        return []

    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        print("  [Oliver Wyman] 响应无 items 列表")
        return []

    if len(items) == 0:
        print("  ⚠ [Oliver Wyman] JSON 返回 0 篇文章。")
        print("    提示：AEM API 小批量可能返回空，已使用 batch=99999 全量请求。")
        print("    如仍为空，请检查 Oliver Wyman 网站是否正常可访问。")
        return []

    total_in_json = len(items)
    articles = []
    seen = set()

    for it in items:
        if not isinstance(it, dict):
            continue
        href = (it.get("path") or it.get("url") or "").strip()
        title = (it.get("title") or "").strip()
        ds = (it.get("date") or "").strip()
        if not href or not title:
            continue

        pub_dt = None
        if ds:
            try:
                pub_dt = datetime.strptime(ds[:10], "%Y-%m-%d")
            except Exception:
                pub_dt = parse_date(ds)
        if pub_dt and pub_dt < cutoff:
            continue

        key = href.split("#", 1)[0].split("?", 1)[0].rstrip("/")
        if key in seen:
            continue
        seen.add(key)

        date_str = pub_dt.strftime("%Y-%m-%d") if pub_dt else ""
        articles.append({
            "url": key,
            "title": title[:200],
            "date": date_str,
            "pub_date": pub_dt,
            "summary": "",
            "source": "oliverwyman_aggregator",
        })

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"  [Oliver Wyman] JSON 共 {total_in_json} 条，"
          f"日期 >= {cutoff.strftime('%Y-%m-%d')} → {len(articles)} 篇")
    return articles


# ════════════════════════════════════════════════════
def fetch_woodmac(list_url, months, cutoff):
    """
    通过 Woodmac 内部 API 拉取 market-insights 全量文章列表。
    from 步进=6（每批实际返回6条），5个UUID合并全局去重。
    """
    SECTION_UUIDS = [
        "ab340f5d-4b25-4aa5-afde-72ea64cb858b",  # 主列表
        "fc5de2ef-997c-476e-a84e-e469b42e442b",
        "a2ed5a36-98fa-4afa-96d8-50f65ee09318",
        "50064872-5d97-4ac8-b420-e987d23b69e0",
        "397d7b69-75ab-41a3-835f-20e91af54f08",
    ]
    API_BASE  = "https://www.woodmac.com/api/v1/search/latest-thinking"
    PAGE_SIZE = 6      # 服务端固定每次返回6条，size参数无效
    STEP      = 6      # from 每次 +6，跳过已取的6条
    MAX_FROM  = 3000   # 最大偏移量防止死循环（total最大约4211）

    headers = {
        "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept":       "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer":      "https://www.woodmac.com/market-insights/",
        "Origin":       "https://www.woodmac.com",
    }

    articles   = []
    global_seen = set()   # 跨UUID全局去重

    print(f"\n  [Woodmac] 开始抓取，共 {len(SECTION_UUIDS)} 个板块"
          f"，步进={STEP}，cutoff={cutoff.strftime('%Y-%m-%d')}")

    for uuid in SECTION_UUIDS:
        api_url       = f"{API_BASE}/{uuid}"
        from_val      = 1
        section_added = 0
        consecutive_no_new = 0   # 连续无新URL计数（防止服务端返回循环数据）

        if CURL_AVAILABLE:
            sess = _req.Session()
        else:
            import requests as _rq
            sess = _rq.Session()

        while from_val <= MAX_FROM:
            try:
                kw = dict(json={"size": PAGE_SIZE, "from": from_val},
                          headers=headers, timeout=20)
                if CURL_AVAILABLE:
                    kw["impersonate"] = "chrome124"
                r = sess.post(api_url, **kw)
            except Exception as e:
                print(f"    [{uuid[:8]}] 请求异常: {e}")
                break

            if r.status_code != 200:
                print(f"    [{uuid[:8]}] HTTP {r.status_code}")
                break

            try:
                rj = r.json()
            except Exception:
                break

            if not rj.get("success"):
                break

            data      = rj.get("data") or []
            total_api = rj.get("total")

            if not data:
                break  # 已到末尾

            added_this   = 0
            oldest_page  = None
            all_old      = True   # 本批是否全部早于 cutoff

            for item in data:
                url_raw  = (item.get("url") or "").strip()
                headline = (item.get("headline") or "").strip()
                date_raw = (item.get("date") or "").strip()
                teaser   = (item.get("teasertext") or "").strip()
                fmt      = (item.get("format") or "").strip()

                if not url_raw or not headline:
                    continue

                if not url_raw.startswith("http"):
                    url_raw = "https://www.woodmac.com" + url_raw
                canon = url_raw.rstrip("/")

                # 全局去重
                if canon in global_seen:
                    continue
                global_seen.add(canon)

                pub_dt   = parse_date(date_raw) if date_raw else None
                date_str = pub_dt.strftime("%Y-%m-%d") if pub_dt else ""

                if pub_dt:
                    oldest_page = pub_dt if oldest_page is None else min(oldest_page, pub_dt)
                    if pub_dt >= cutoff:
                        all_old = False
                    else:
                        continue   # 超出范围，跳过但继续处理本批

                articles.append({
                    "url":        url_raw,
                    "title":      headline[:200],
                    "date":       date_str,
                    "pub_date":   pub_dt,
                    "summary":    teaser[:500],
                    "source":     "woodmac",
                    "_wm_format": fmt,
                })
                added_this    += 1
                section_added += 1

            # 进度打印（每10批打一次，避免刷屏）
            batch_num = (from_val - 1) // STEP + 1
            if batch_num % 10 == 1 or added_this > 0:
                print(f"    [{uuid[:8]}] from={from_val:>4}  本批新增={added_this}  "
                      f"板块累计={section_added:>3}  总计={len(articles):>3}"
                      + (f"  oldest={oldest_page.strftime('%Y-%m-%d')}" if oldest_page else "")
                      + (f"  total={total_api}" if total_api and from_val == 1 else ""))

            # 终止条件1：本批新URL全部早于 cutoff（且确认有日期）
            if all_old and oldest_page and oldest_page < cutoff:
                print(f"    [{uuid[:8]}] → 已过 cutoff，停止（from={from_val}）")
                break

            # 终止条件2：连续5批无新URL（去重后），说明服务端在循环返回
            if added_this == 0:
                consecutive_no_new += 1
                if consecutive_no_new >= 5:
                    print(f"    [{uuid[:8]}] → 连续{consecutive_no_new}批无新URL，停止")
                    break
            else:
                consecutive_no_new = 0

            # 终止条件3：已超过 total
            if total_api and from_val >= total_api:
                print(f"    [{uuid[:8]}] → 已达 total={total_api}，停止")
                break

            from_val += STEP
            time.sleep(0.15)   # 轻度限速，约 40req/s，不会触发封禁

        print(f"    [{uuid[:8]}] 板块完成，新增 {section_added} 篇，"
              f"from 最终={from_val}")

    articles.sort(key=lambda x: x.get("date") or "", reverse=True)
    print(f"\n  [Woodmac] 共收集 {len(articles)} 篇（{months}个月内），"
          f"全局去重后 URL 数={len(global_seen)}")
    return articles


def _body_text_items(body):
    out = []
    for item in (body or []):
        if not isinstance(item, dict):
            continue
        txt = (item.get("text") or "").strip()
        if txt:
            out.append({"tag": item.get("tag", "p") or "p", "text": txt})
    return out


def _body_total_chars(body):
    return sum(len(x["text"]) for x in _body_text_items(body))


def _body_effective_paras(body):
    return sum(1 for x in _body_text_items(body) if len(x["text"]) >= 60)


def _text_looks_linkish(text):
    t = (text or "").strip()
    if not t:
        return False
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9'/-]*", t)
    if len(t) <= 120 and len(words) <= 16 and not re.search(r"[.!?;:]", t):
        return True
    return False


def _text_has_ellipsis(text):
    t = (text or "").strip().rstrip('"').rstrip("'").rstrip("”").rstrip("’").strip()
    return t.endswith("...") or t.endswith("…")


def _extract_blocks_from_html_fragment(html_text):
    if not html_text:
        return []
    try:
        import html as _html_mod
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        blocks = []
        for el in soup.find_all(["p", "h2", "h3", "h4", "li", "blockquote"]):
            txt = _html_mod.unescape(el.get_text(" ", strip=True))
            if txt and len(txt) > 12:
                tag_name = el.name if el.name != "blockquote" else "p"
                blocks.append({"tag": tag_name, "text": txt})
        return blocks
    except Exception:
        return []


def _best_effort_preview_blocks(article):
    _api_body = (article.get("_api_body_html") or "").strip()
    if _api_body:
        _blocks = _extract_blocks_from_html_fragment(_api_body)
        if _blocks:
            return _blocks
    summary = (article.get("summary") or "").strip()
    if summary:
        return [{"tag": "p", "text": summary}]
    return []


def _preview_links(article, limit=8):
    out = []
    seen = set()
    raw = (article.get("_api_body_html") or "").strip()
    if not raw:
        return out
    def _looks_like_person_name(txt):
        t = (txt or "").strip()
        if not t or len(t) > 40:
            return False
        words = re.findall(r"[A-Za-zÀ-ÿ'’-]+", t)
        if len(words) not in (2, 3):
            return False
        if any(len(w) <= 1 for w in words):
            return False
        caps = 0
        for w in words:
            if w[:1].isupper() and w[1:].replace("’", "").replace("'", "").replace("-", "").isalpha():
                caps += 1
        return caps == len(words)
    def _is_article_href(href):
        h = (href or "").lower()
        return ("/our-insights/" in h or "/our-thinking/" in h or
                "/capabilities/" in h or "/industries/" in h or
                "/mhi/" in h or "/quarterly/" in h)
    try:
        soup = BeautifulSoup(raw, "html.parser")
        for a in soup.find_all("a", href=True):
            txt = a.get_text(" ", strip=True)
            href = (a.get("href") or "").strip()
            if not txt or len(txt) < 4:
                continue
            if href.startswith("/"):
                href = "https://www.mckinsey.com" + href
            if not href.startswith("http"):
                continue
            if "/our-people/" in href:
                continue
            if _looks_like_person_name(txt) and not _is_article_href(href):
                continue
            key = (txt.lower(), href.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append({"text": txt, "href": href})
            if len(out) >= limit:
                break
    except Exception:
        return []
    return out




_MCK_THEME_FEED_CACHE = {"ts": 0.0, "items": []}

def _mck_theme_feed_cards(article, limit=6):
    """McKinsey themes 兜底：补充同主题近期条目，避免页面过短。"""
    low_url = (article.get("url", "") or "").lower()
    if "mckinsey.com" not in low_url or "/featured-insights/themes/" not in low_url:
        return []
    try:
        import html as _hm
        now_ts = time.time()
        cache = _MCK_THEME_FEED_CACHE.get("items") or []
        if cache and (now_ts - float(_MCK_THEME_FEED_CACHE.get("ts") or 0.0) < 1800):
            pass
        else:
            hdrs = {
                "Accept": "application/json",
                "Origin": "https://www.mckinsey.com",
                "Referer": "https://www.mckinsey.com/featured-insights/themes",
            }
            data_items = []
            for pg in (1, 2):
                api = f"https://prd-api.mckinsey.com/v1/blogs/themes/{pg}"
                r = None
                if CURL_AVAILABLE:
                    try:
                        sess = _get_curl_session("chrome124")
                        r = sess.get(api, impersonate="chrome124", timeout=20, headers=hdrs)
                    except Exception:
                        r = None
                if r is None:
                    r = http_get(api, timeout=20, headers=hdrs)
                if not r or r.status_code != 200:
                    continue
                rows = (r.json() or {}).get("results", [])
                if not rows:
                    continue
                for it in rows:
                    _u = (it.get("url") or "").strip()
                    if not _u:
                        continue
                    if _u.startswith("/"):
                        _u = "https://www.mckinsey.com" + _u
                    data_items.append({
                        "url": _u.split("?")[0].rstrip("/"),
                        "title": _hm.unescape((it.get("title") or "").strip()),
                        "summary": _hm.unescape((it.get("description") or "").strip()),
                        "date": (it.get("displaydate") or "")[:10],
                    })
            seen = set()
            dedup = []
            for it in data_items:
                u = it.get("url") or ""
                if not u or u in seen:
                    continue
                seen.add(u)
                dedup.append(it)
            _MCK_THEME_FEED_CACHE["items"] = dedup
            _MCK_THEME_FEED_CACHE["ts"] = now_ts
            cache = dedup

        out = []
        cur = (article.get("url") or "").split("?")[0].rstrip("/").lower()
        for it in cache:
            u = (it.get("url") or "").lower()
            t = (it.get("title") or "").strip()
            if not t or not u or u == cur:
                continue
            desc = (it.get("summary") or "").strip()
            if len(desc) > 220:
                desc = desc[:217].rstrip() + "..."
            out.append({
                "href": it.get("url") or "",
                "text": t,
                "desc": desc,
                "date": it.get("date") or "",
            })
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []

def _preview_payload(article, body=None):
    blocks = _best_effort_preview_blocks(article)
    if not blocks:
        blocks = _body_text_items(body)
    links = _preview_links(article)
    note = "Preview text only. Full article retrieval was blocked."
    return {
        "blocks": blocks[:8],
        "links": links[:8],
        "note": note,
    }


def _body_looks_search_snippets(article, body):
    items = _body_text_items(body)
    if len(items) < 4:
        return False
    low_url = (article.get("url", "") or "").lower()
    is_mck_theme = "mckinsey.com" in low_url and "/featured-insights/themes/" in low_url
    if not is_mck_theme:
        return False
    domain_hits = 0
    source_hits = 0
    shortish = 0
    for item in items[:20]:
        txt = (item.get("text") or "").strip().lower()
        if not txt:
            continue
        if "mckinsey.com/" in txt or "https://" in txt or "www." in txt:
            domain_hits += 1
        if "mckinsey & company" in txt or "site:mckinsey.com" in txt:
            source_hits += 1
        if len(txt) <= 220:
            shortish += 1
    return (
        domain_hits >= 3
        or source_hits >= 3
        or (domain_hits + source_hits >= 4 and shortish >= 4)
    )


def _normalize_mck_theme_body(article, body):
    low_url = (article.get("url", "") or "").lower()
    is_mck_theme = "mckinsey.com" in low_url and "/featured-insights/themes/" in low_url
    if not is_mck_theme:
        return body or []
    api_blocks = _best_effort_preview_blocks(article)
    if not body:
        return api_blocks or []
    if _body_looks_search_snippets(article, body):
        return api_blocks or body
    if _body_looks_teaser(article, body) and api_blocks:
        if _body_total_chars(api_blocks) >= max(180, _body_total_chars(body) * 0.8):
            return api_blocks
    return body


def _title_words(text):
    toks = re.findall(r"[a-z0-9]+", (text or "").lower())
    stop = {
        "the", "and", "for", "with", "from", "into", "that", "this", "your",
        "their", "what", "when", "where", "more", "less", "over", "under",
        "about", "have", "will", "than", "amid", "after", "before", "inside",
        "mckinsey", "featured", "insights", "themes",
    }
    return {t for t in toks if len(t) > 2 and t not in stop}


def _title_matches_expected(found, expected, url=""):
    wf = _title_words(found)
    we = _title_words(expected) or _title_words(url.rstrip("/").split("/")[-1].replace("-", " "))
    if not wf or not we:
        return True
    inter = len(wf & we)
    need = 2 if min(len(wf), len(we)) <= 4 else max(2, min(len(wf), len(we)) // 2)
    return inter >= need


def _body_looks_teaser(article, body):
    items = _body_text_items(body)
    if not items:
        return True
    total = sum(len(x["text"]) for x in items)
    paras = sum(1 for x in items if len(x["text"]) >= 60)
    linkish = sum(1 for x in items if _text_looks_linkish(x["text"]))
    last_two = items[-2:] if len(items) >= 2 else items
    ellipsis = any(_text_has_ellipsis(x["text"]) for x in last_two)
    is_mck_theme = "mckinsey.com" in (article.get("url", "") or "") and "/featured-insights/themes/" in (article.get("url", "") or "")
    if total < 420:
        return True
    if paras <= 2 and total < 900:
        return True
    if ellipsis and total < 1600:
        return True
    if linkish >= 4 and linkish >= max(2, len(items) // 2):
        return True
    if is_mck_theme and paras <= 3 and total < 1300:
        return True
    return False


def _mck_page_quality_ok(url, html_text, article=None):
    article = article or {}
    if not html_text:
        return False, "no_html"
    low = html_text[:5000].lower()
    if any(x in low for x in ["access denied", "just a moment", "performing security verification", "cf-challenge"]):
        return False, "blocked_page"
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["nav", "header", "footer", "script", "style", "noscript"]):
            tag.decompose()
        h1 = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "").strip()
        exp = (article.get("title") or "").strip()
        if h1 and exp and not _title_matches_expected(h1, exp, url):
            return False, "title_mismatch"
        blocks = []
        linkish = 0
        for el in soup.find_all(["p", "h2", "h3", "h4", "li"]):
            txt = el.get_text(" ", strip=True)
            if not txt or len(txt) < 24:
                continue
            if any(s in txt.lower() for s in ["cookie", "privacy", "newsletter", "related articles", "skip to main content"]):
                continue
            blocks.append({"tag": el.name, "text": txt})
            if _text_looks_linkish(txt):
                linkish += 1
            if len(blocks) >= 120:
                break
        _is_theme = "/featured-insights/themes/" in (url or "")
        _page_text = soup.get_text(" ", strip=True).lower()[:12000]
        if _is_theme and h1:
            _body_chars = _body_total_chars(blocks)
            _paras = _body_effective_paras(blocks)
            _theme_shell = ("back to mckinsey themes" in _page_text) or ("more from mckinsey" in _page_text)
            if _theme_shell and (_body_chars >= 220 or _paras >= 1 or linkish >= 3):
                return True, "theme_hub_page"
        if _body_looks_teaser({"url": url, "title": h1 or exp}, blocks):
            return False, "teaser_or_linkhub"
        if _body_total_chars(blocks) < 800:
            return False, "too_thin"
        return True, ""
    except Exception as e:
        return False, f"parse_error:{e}"


def fetch_article_body(url, debug=False, article=None):
    """
    抓取文章正文和日期
    策略1：curl_cffi/requests 直接请求（快，无弹窗）
    策略2：如果返回 403 或正文为空，用同步 Playwright 渲染
    article: 可选，用于日期校验和 teaser 判断
    """
    html = None
    used_playwright = False
    pw_extracted_date = ""

    # ── 工具函数：判断 HTML 是否有实质正文（需在 CF 块之前定义）──────
    def _has_real_body(html_text):
        if not html_text or len(html_text) < 2000:
            return False
        try:
            _s = BeautifulSoup(html_text, "html.parser")
            for tag in _s(["nav","header","footer","script","style","noscript"]):
                tag.decompose()
            paras = [p.get_text(strip=True) for p in _s.find_all("p")
                     if len(p.get_text(strip=True)) > 40]
            return len(paras) >= 3
        except Exception:
            return False

    # ── McKinsey 快速通道 ────────────────────────────────────────────────────
    # 用 curl_cffi（TLS 指纹伪装）尝试获取正文，不走慢速回退链
    if "mckinsey.com" in url:
        _mck_html = None
        # 尝试 1: curl_cffi（可能被 Akamai 封锁）
        try:
            time.sleep(random.uniform(1.0, 2.5))
            r = http_get(url, timeout=15)
            if r and r.status_code == 200 and "Access Denied" not in r.text[:500]:
                _mck_html = r.text
        except Exception:
            pass
        # 尝试 2: requests + HTTP/1.1（Akamai 有时放行 HTTP/1.1）
        if not _mck_html:
            try:
                import requests as _rq
                _headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                _r2 = _rq.get(url, headers=_headers, timeout=15, allow_redirects=True)
                if _r2.status_code == 200 and "Access Denied" not in _r2.text[:500]:
                    _mck_html = _r2.text
            except Exception:
                pass
        if _mck_html:
            try:
                from bs4 import BeautifulSoup as _BS
                _s = _BS(_mck_html, "html.parser")
                _title = ""
                _t = _s.find("h1")
                if _t:
                    _title = _t.get_text(strip=True)
                for tag in _s(["nav","header","footer","script","style","noscript"]):
                    tag.decompose()
                _body = []
                _container = (_s.select_one(".mdc-o-content-body") or _s.select_one("[data-test-id='article-body']") or _s.select_one("article") or _s.select_one("[class*='article']") or _s.select_one("main"))
                _src = _container if _container else _s
                for el in _src.find_all(["p","h2","h3","h4","li"]):
                    txt = el.get_text(strip=True)
                    if txt and len(txt) > 30:
                        _body.append({"tag": el.name, "text": txt})
                _art_ctx = {"url": url, "title": _title or (article or {}).get("title", "")}
                if _body and not _body_looks_teaser(_art_ctx, _body):
                    return (_title, _body, "")
            except Exception:
                pass

        # 尝试 3: Archive.org 正文（Akamai 全挡时至少拿到确定性历史正文）
        try:
            import urllib.parse as _up_mck
            import requests as _rq_arch
            _mck_arch_api = f"https://archive.org/wayback/available?url={_up_mck.quote(url, safe='')}"
            _arch_url = ""
            _arch_hdr = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
            try:
                _mck_arch = _rq_arch.get(_mck_arch_api, timeout=15, headers=_arch_hdr)
                if _mck_arch.status_code == 200:
                    _snap = (_mck_arch.json() or {}).get("archived_snapshots", {}).get("closest", {})
                    if _snap.get("available"):
                        _arch_url = (_snap.get("url") or "").replace("http://", "https://")
            except Exception:
                pass
            if not _arch_url:
                try:
                    _direct_wb = f"https://web.archive.org/web/2026/{url}"
                    _dr = _rq_arch.head(_direct_wb, timeout=12, allow_redirects=True, headers=_arch_hdr)
                    if _dr.status_code == 200 and "web.archive.org" in _dr.url:
                        _arch_url = _dr.url
                except Exception:
                    pass
            if _arch_url:
                # 校验快照日期：若快照比文章发布日早超过60天，跳过Archive.org
                import re as _re_arch2
                _snap_m2 = _re_arch2.search(r'/web/(\d{8})\d*/', _arch_url)
                if _snap_m2:
                    try:
                        _snap_dt2 = datetime.strptime(_snap_m2.group(1), "%Y%m%d")
                        _art_date2 = (article or {}).get("date", "") or ""
                        if _art_date2:
                            _art_dt2 = datetime.strptime(_art_date2[:10], "%Y-%m-%d")
                            if (_art_dt2 - _snap_dt2).days > 60:
                                print(f"         ⚠ 正文Archive快照({_snap_m2.group(1)})过旧，跳过")
                                _arch_url = ""
                    except Exception:
                        pass
            if _arch_url:
                _ar = _rq_arch.get(_arch_url, timeout=30, headers=_arch_hdr)
                if _ar.status_code == 200:
                    from bs4 import BeautifulSoup as _BS
                    _s = _BS(_ar.text, "html.parser")
                    for _wb in ["#wm-ipp-base", "#wm-ipp", "#donato"]:
                        _el = _s.select_one(_wb)
                        if _el:
                            _el.decompose()
                    _title = (_s.find("h1").get_text(" ", strip=True) if _s.find("h1") else "")
                    _body = []
                    _container = _s.select_one("article") or _s.select_one("main") or _s
                    for el in _container.find_all(["p", "h2", "h3", "h4", "li"]):
                        txt = el.get_text(" ", strip=True)
                        if txt and len(txt) > 30:
                            _body.append({"tag": el.name, "text": txt})
                    if _body and not _body_looks_teaser({"url": url, "title": _title}, _body):
                        print(f"         → McKinsey Archive 正文成功（{len(_body)}段）")
                        return (_title, _body, "")
        except Exception as _ae:
            print(f"         → McKinsey Archive 正文失败: {_ae}")

        # ── McKinsey 搜索引擎摘要降级 ──────────────────────────────────
        _slug_mck = url.rstrip("/").split("/")[-1].replace("-", " ")
        _mck_body = []

        # 不再使用 Google Cache：自动化请求会触发「异常流量」验证码页，截图为废图

        # DuckDuckGo
        try:
            _ddg_q = _slug_mck.replace(" ", "+") + "+site:mckinsey.com"
            _ddg_h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                      "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
            _ddg_r = http_get(f"https://html.duckduckgo.com/html/?q={_ddg_q}",
                              headers=_ddg_h, timeout=15)
            if _ddg_r and _ddg_r.status_code == 200:
                _ddg_soup = BeautifulSoup(_ddg_r.text, "html.parser")
                for _res in _ddg_soup.select(".result__body, .result__snippet, .result__a"):
                    _t = _res.get_text(strip=True)
                    _parent = _res.find_parent(class_=lambda c: c and "result" in c)
                    _link = (_parent.find("a", class_="result__url") if _parent else None)
                    _link_txt = _link.get_text(strip=True) if _link else ""
                    if len(_t) > 30 and ("mckinsey.com" in _link_txt or not _link_txt):
                        _mck_body.append({"tag": "p", "text": _t})
                if _mck_body:
                    print(f"         → DuckDuckGo 摘要成功（{len(_mck_body)}段）")
        except Exception as _de:
            print(f"         → DuckDuckGo 失败: {_de}")

        # Bing
        if not _mck_body:
            try:
                _bq = _slug_mck.replace(" ", "+") + "+site:mckinsey.com"
                _bh = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                       "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
                _br = http_get(f"https://www.bing.com/search?q={_bq}", headers=_bh, timeout=15)
                if _br and _br.status_code == 200:
                    _bs = BeautifulSoup(_br.text, "html.parser")
                    for sel in [".b_caption p", ".b_snippet", ".b_algoSlug",
                                "[class*='snippet']", "[class*='caption']"]:
                        for el in _bs.select(sel):
                            _t = el.get_text(strip=True)
                            if len(_t) > 30:
                                _mck_body.append({"tag": "p", "text": _t})
                    if _mck_body:
                        print(f"         → Bing 摘要成功（{len(_mck_body)}段）")
            except Exception as _be:
                print(f"         → Bing 失败: {_be}")

        if _mck_body:
            _mck_title = " ".join(w.capitalize() for w in _slug_mck.split())
            return (_mck_title, _mck_body, "")

        return ("", [], "")

    # ── CF Turnstile 硬拦截：最早期拦截 ────────────────────────────────────────
    # 优先级：ScrapingBee（真实页面）> DuckDuckGo > Bing > 兜底
    if any(d in url for d in CF_TURNSTILE_DOMAINS):
        _slug   = url.rstrip("/").split("/")[-1].replace("-", " ")
        _domain = urlparse(url).netloc.replace("www.", "").split(".")[0].capitalize()

        # 1. ScrapingBee：真实浏览器 + 住宅 IP，绕过 CF Turnstile（首选）
        if SCRAPINGBEE_KEY:
            try:
                import urllib.parse as _up
                # McKinsey 需要 stealth_proxy，其他 CF 域名用 premium_proxy
                _use_stealth = "mckinsey.com" in url
                _sb_params = {
                    "api_key":         SCRAPINGBEE_KEY,
                    "url":             url,
                    "render_js":       "true",
                    "stealth_proxy":   "true" if _use_stealth else "false",
                    "premium_proxy":   "false" if _use_stealth else "true",
                    "country_code":    "us" if _use_stealth else "",
                    "block_ads":       "true",
                    "block_resources": "false",
                    "wait":            "5000" if _use_stealth else "3000",
                    "screenshot":      "false",
                }
                _sb_url = "https://app.scrapingbee.com/api/v1/?" + _up.urlencode(_sb_params)
                print(f"         → ScrapingBee 请求中...")
                # 用标准 requests 避免 curl_cffi URL 长度导致 500
                try:
                    import requests as _rq_sb
                    _sb_r = _rq_sb.get(_sb_url, timeout=60)
                except Exception:
                    _sb_r = http_get(_sb_url, headers={"Accept": "text/html,*/*"}, timeout=60)
                if _sb_r and _sb_r.status_code == 200 and _has_real_body(_sb_r.text):
                    print(f"         → ScrapingBee 成功（{len(_sb_r.text)}字节），走正常解析")
                    html = _sb_r.text
                    _force_playwright = False
                    # html 已设置，跳过搜索引擎摘要，直接走后续 soup 解析
                elif _sb_r and _sb_r.status_code == 200:
                    print(f"         → ScrapingBee 内容不足，降级搜索引擎")
                else:
                    print(f"         → ScrapingBee 失败(HTTP {_sb_r.status_code if _sb_r else 0})，降级搜索引擎")
            except Exception as _sbe:
                print(f"         → ScrapingBee 异常: {_sbe}，降级搜索引擎")

        # ScrapingBee 成功时跳过搜索引擎摘要，直接走 html 解析
        if html is None:
            _cf_body = []
            _cf_date = ""

            # 2. DuckDuckGo
            try:
                _ddg_q = _slug.replace(" ", "+") + "+site:" + urlparse(url).netloc
                _ddg_h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                          "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
                _ddg_r = http_get(f"https://html.duckduckgo.com/html/?q={_ddg_q}", headers=_ddg_h, timeout=15)
                if _ddg_r and _ddg_r.status_code == 200:
                    _ddg_soup = BeautifulSoup(_ddg_r.text, "html.parser")
                    for _res in _ddg_soup.select(".result__body, .result__snippet, .result__a"):
                        _t = _res.get_text(strip=True)
                        _parent = _res.find_parent(class_=lambda c: c and "result" in c)
                        _link = (_parent.find("a", class_="result__url") if _parent else None)
                        _link_txt = _link.get_text(strip=True) if _link else ""
                        if len(_t) > 30 and (urlparse(url).netloc.replace("www.","") in _link_txt or not _link_txt):
                            _cf_body.append({"tag": "p", "text": _t})
                    if _cf_body:
                        print(f"         → DuckDuckGo 摘要成功（{len(_cf_body)}段）")
            except Exception as _de:
                print(f"         → DuckDuckGo 失败: {_de}")

            # 3. Bing 备用
            if not _cf_body:
                try:
                    _bq = _slug.replace(" ", "+") + "+site:" + urlparse(url).netloc
                    _bh = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                           "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}
                    _br = http_get(f"https://www.bing.com/search?q={_bq}", headers=_bh, timeout=15)
                    if _br and _br.status_code == 200:
                        _bs = BeautifulSoup(_br.text, "html.parser")
                        for sel in [".b_caption p", ".b_snippet", ".b_algoSlug",
                                     "[class*='snippet']", "[class*='caption']"]:
                            for el in _bs.select(sel):
                                _t = el.get_text(strip=True)
                                if len(_t) > 30:
                                    _cf_body.append({"tag": "p", "text": _t})
                        if _cf_body:
                            print(f"         → Bing 摘要成功（{len(_cf_body)}段）")
                except Exception as _be:
                    print(f"         → Bing 失败: {_be}")

            # 4. 标题/来源/日期
            _cf_title = " ".join(w.capitalize() for w in _slug.split())
            _cf_body.append({"tag": "h3", "text": "Source"})
            _cf_body.append({"tag": "p",
                "text": f"⚠ This article is protected by Cloudflare ({_domain}). "
                        f"Content retrieved via search engine summaries."})
            _cf_body.append({"tag": "p", "text": f"Original article: {url}"})
            _date_m = re.search(r'/(20\d{2})[/_-](\d{2})[/_-](\d{2})/', url)
            if _date_m:
                _cf_date = f"{_date_m.group(1)}-{_date_m.group(2)}-{_date_m.group(3)}"

            print(f"         → CF搜索引擎兜底：{len(_cf_body)}段，标题: {_cf_title[:50]}")
            return _cf_title, _cf_body, _cf_date
        # ScrapingBee 成功：html 已设置，继续走正常 HTML 解析（不 return）

    # S&P Global 文章页面正文全靠 JS 渲染，curl_cffi 只能拿到导航壳
    # 必须强制走 Playwright，否则永远是空白页
    # 这些站点需要 patchright 渲染：JS 动态内容 或 Cloudflare 反爬
    _CF_DOMAINS = ["spglobal.com", "bain.com", "mckinsey.com", "bcg.com",
                   "kearney.com"]
    _force_playwright = any(d in url for d in _CF_DOMAINS)

    def _has_real_body(html_text):  # 已在函数开头定义，此处保留兼容
        if not html_text or len(html_text) < 2000: return False
        try:
            _s = BeautifulSoup(html_text, "html.parser")
            for tag in _s(["nav","header","footer","script","style","noscript"]): tag.decompose()
            paras = [p.get_text(strip=True) for p in _s.find_all("p") if len(p.get_text(strip=True)) > 40]
            return len(paras) >= 3
        except Exception: return False

    # ── Cloudflare Turnstile 硬拦截站点 ───────────────────────────────────
    # bain.com / mckinsey.com / bcg.com 等使用 CF Turnstile，
    # 任何自动化浏览器（包括 patchright headless=False）均无法通过验证。
    # 策略：依次尝试 Archive.org → Bing 搜索摘要（不用 Google Cache，易触发验证码）
    # 最终兜底：用搜索引擎摘要拼凑正文，保证 PDF/截图有内容而非空白
    # kearney.com 已移除：patchright headless=False 可通过 CF 验证
    _CF_HARDBLOCK = ["bain.com", "bcg.com"]
    _is_cf_hardblock = any(d in url for d in _CF_HARDBLOCK)

    if _is_cf_hardblock:
        # 1. Archive.org (API → direct fallback)
        _ah = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
        _aurl = ""
        try:
            _ar = http_get(f"https://archive.org/wayback/available?url={url}",
                           headers=_ah, timeout=12)
            if _ar and _ar.status_code == 200:
                _snap = (_ar.json() or {}).get("archived_snapshots", {}).get("closest", {})
                if _snap.get("status") == "200":
                    _aurl = _snap.get("url", "")
        except Exception:
            pass
        if not _aurl:
            try:
                import requests as _std_rq
                _direct = f"https://web.archive.org/web/2026/{url}"
                _dr = _std_rq.head(_direct, timeout=10, allow_redirects=True,
                                   headers={"User-Agent": "Mozilla/5.0"})
                if _dr.status_code == 200:
                    _aurl = _dr.url
            except Exception:
                pass
        if _aurl:
            try:
                print(f"         → Archive.org 快照: {_aurl[-40:]}")
                _ar2 = http_get(_aurl, headers=_ah, timeout=25)
                if _ar2 and _ar2.status_code == 200 and _has_real_body(_ar2.text):
                    print(f"         → Archive.org 内容完整（{len(_ar2.text)}字节）")
                    html = _ar2.text
                    _force_playwright = False
                else:
                    print(f"         → Archive.org 内容不足")
            except Exception as _ae:
                print(f"         → Archive.org 读取失败: {_ae}")
        else:
            print(f"         → Archive.org 无快照")

        # 2. Bing 搜索摘要（最终兜底，修复版）
        if html is None:
            try:
                _title_hint = url.rstrip("/").split("/")[-1].replace("-", " ")
                _bh = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                _bq = _title_hint.replace(" ", "+") + "+site:" + urlparse(url).netloc
                _br = http_get(f"https://www.bing.com/search?q={_bq}", headers=_bh, timeout=15)
                if _br and _br.status_code == 200:
                    _bsoup = BeautifulSoup(_br.text, "html.parser")
                    _snippets = []
                    # 多种 selector 兜底，Bing 结构经常变
                    for sel in [".b_caption p", ".b_algoSlug", ".b_snippet",
                                 "[class*='snippet']", "[class*='caption']"]:
                        for el in _bsoup.select(sel):
                            _t = el.get_text(strip=True)
                            if len(_t) > 40 and _t not in _snippets:
                                _snippets.append(_t)
                    if _snippets:
                        print(f"         → Bing 摘要获取成功（{len(_snippets)}段）")
                        html = "<html><body>" + "".join(
                            f"<p>{s}</p>" for s in _snippets[:10]
                        ) + f"<p>原文: <a href='{url}'>{url}</a></p></body></html>"
                        _force_playwright = False
                    else:
                        print(f"         → Bing 无有效摘要（HTTP {_br.status_code}）")
            except Exception as _be:
                print(f"         → Bing 搜索失败: {_be}")

        # 4. 所有在线来源都失败 → 使用摘要兜底，不再尝试 Playwright
        if html is None:
            print(f"         → CF Turnstile：所有来源失败，用 article.summary 兜底")
        # 无论 html 是否有值，都不再走 Playwright（CF 已知无法绕过）
        _force_playwright = False

    # 策略1：requests 直接抓（CF 域名 / S&P Global 强制跳过）
    # 注意：如果上面 CF 备选来源（Archive.org / Bing）已拿到 html，直接跳过
    r = http_get(url) if (not _force_playwright and html is None) else None
    # 检测 404 / 已迁移页面，跳过无意义的 Playwright 渲染
    _is_404_or_moved = False
    if r and r.status_code in (404, 410):
        _is_404_or_moved = True
        print(f"         → HTTP {r.status_code}，文章已移除或迁移")
    elif r and r.status_code == 200 and r.text:
        _low = r.text[:5000].lower()
        if ("<title>404" in _low or
            "has moved on" in _low or
            "page you're looking for is no longer available" in _low or
            "this page has moved" in _low):
            _is_404_or_moved = True
            print(f"         → 页面内容为 404/迁移提示，文章已下线")
    if _is_404_or_moved:
        return ("", [], "")
    if r and r.status_code == 200 and _has_real_body(r.text):
        html = r.text
    elif html is None:
        # 策略2：Playwright 渲染（JS 渲染 / 403 / S&P Global 强制）
        status = r.status_code if r else ("forced" if _force_playwright else 0)
        if _force_playwright:
            print(f"         → S&P Global 强制 Playwright 渲染（JS 动态内容）...")
        else:
            print(f"         → requests {status}，切换到 Playwright...")
        try:
            import subprocess as _sp, tempfile as _tf, os as _os
            # 用子进程跑同步 Playwright 避免 asyncio 冲突
            script = f"""
import sys, re, json
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def try_extract_date(soup, url):
    # meta 标签
    for name in ["article:published_time","og:article:published_time",
                 "datePublished","pubdate","publish-date","date"]:
        t = (soup.find("meta", attrs={{"property":name}}) or
             soup.find("meta", attrs={{"name":name}}) or
             soup.find("meta", attrs={{"itemprop":name}}))
        if t and t.get("content"):
            v = t["content"][:20]
            if re.search(r"20\\d{{2}}", v): return v
    # JSON-LD
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(s.string or "")
            if isinstance(d, list): d = d[0]
            for k in ["datePublished","dateCreated","dateModified"]:
                if d.get(k): return str(d[k])[:20]
        except Exception: pass
    # time tag
    for t in soup.find_all("time"):
        v = t.get("datetime") or t.get_text(strip=True)
        if v and re.search(r"20\\d{{2}}", v): return v[:20]
    # class-based
    for sel in ["[class*='date']","[class*='published']","[class*='created']",
                "[class*='posted']","[itemprop='datePublished']","[data-date]",
                ".article-date",".post-date",".news-date",".pubdate",".timestamp",
                "[class*='ArticleDate']","[class*='article-date']","[class*='pub-date']"]:
        try:
            el = soup.select_one(sel)
            if el:
                v = el.get("datetime") or el.get("data-date") or el.get_text(strip=True)
                if v and re.search(r"20\\d{{2}}", v): return v[:30]
        except Exception: pass
    # URL path
    m = re.search(r"/(20\\d{{2}})[/\\-](\\d{{1,2}})[/\\-](\\d{{1,2}})/", url)
    if m: return f"{{m.group(1)}}-{{m.group(2).zfill(2)}}-{{m.group(3).zfill(2)}}"
    # text scan
    text = soup.get_text()[:8000]
    for pat in [
        r"(\\d{{1,2}})\\s+(January|February|March|April|May|June|July|August|September|October|November|December)\\s+(20\\d{{2}})",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\\s+(\\d{{1,2}}),?\\s+(20\\d{{2}})",
        r"(20\\d{{2}})[.\\-/](\\d{{1,2}})[.\\-/](\\d{{1,2}})",
        r"(\\d{{1,2}})[./](\\d{{1,2}})[./](20\\d{{2}})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m: return m.group(0)
    return ""

try:
    from patchright.sync_api import sync_playwright
except ImportError:
    from playwright.sync_api import sync_playwright
_CF_DOMAINS_BODY = ["bain.com","mckinsey.com","bcg.com","kearney.com",
                    "accenture.com","deloitte.com","pwc.com","kpmg.com"]
_is_cf_domain = any(d in {repr(url)} for d in _CF_DOMAINS_BODY)
# kearney.com 用 headless=False（CF 验证可通过）
_launch_args = ["--no-sandbox","--disable-blink-features=AutomationControlled","--start-maximized"]
with sync_playwright() as p:
    # CF 保护域名：用 headless=False 绕过 JS Challenge；其他用 headless=True 更快
    browser = p.chromium.launch(
        headless=not _is_cf_domain,
        args=_launch_args,
        slow_mo=80 if _is_cf_domain else 0,
    )
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="en-US",
        java_script_enabled=True,
        viewport={{"width":1280,"height":900}},
    )
    page = ctx.new_page()
    page.goto({repr(url)}, wait_until="domcontentloaded", timeout=60000)
    is_spg = "spglobal.com" in {repr(url)}
    if _is_cf_domain:
        # 等 CF JS Challenge 自动通过（最多 25 秒）
        for _w in range(25):
            page.wait_for_timeout(1000)
            _pc = page.content()
            _title = page.title()
            _cf_still = any(x in _pc or x in _title for x in [
                "Performing security verification", "Just a moment",
                "Checking your browser", "Verifying you are human",
                "Please wait", "cf-challenge"
            ])
            if not _cf_still:
                break
        page.wait_for_timeout(3000)   # 验证通过后等正文渲染
    elif is_spg:
        page.wait_for_timeout(4000)
        for body_sel in ["article", "main", ".article-body", ".content-body",
                         "[class*='article']", "[class*='content']", ".blog-content"]:
            try:
                el = page.locator(body_sel).first
                el.wait_for(timeout=3000, state="visible")
                break
            except Exception: pass
        page.wait_for_timeout(1500)
    else:
        page.wait_for_timeout(2000)
    for sel in ["button:has-text('Accept All')", "button:has-text('Accept all')",
                "button:has-text('Accept')", "button:has-text('OK')",
                "button:has-text('Agree')",
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=800):
                btn.click()
                page.wait_for_timeout(600)
                break
        except Exception: pass
    page.wait_for_timeout(1000 if not is_spg else 1500)
    html = page.content()
    soup = html_to_soup(html)
    date_found = try_extract_date(soup, {repr(url)})
    # 输出格式: |||DATE:日期值||| + HTML内容
    print(f"|||DATE:{{date_found}}|||", end="")
    print(html, end="")
    browser.close()
"""
            tmp = _tf.NamedTemporaryFile(mode='w', suffix='.py',
                                         delete=False, encoding='utf-8')
            tmp.write(script)
            tmp.close()
            result = _sp.run([sys.executable, tmp.name],
                             capture_output=True, text=True, timeout=90)
            _os.unlink(tmp.name)
            if result.stdout and len(result.stdout) > 1000:
                raw = result.stdout
                # 提取子进程输出的日期标记
                import re as _re
                date_m = _re.search(r"[|]{3}DATE:(.*?)[|]{3}", raw)
                if date_m:
                    _pw_date = date_m.group(1).strip()
                    html = raw[raw.index("|||DATE:") + len(f"|||DATE:{_pw_date}|||"):]
                    # 存到局部变量，后面日期提取会用到
                    if _pw_date and "20" in _pw_date:
                        pw_extracted_date = _pw_date
                else:
                    html = raw
                used_playwright = True
        except Exception as e:
            print(f"         ⚠ Playwright降级失败: {e}")

    if not html:
        return None, [], ""

    soup = html_to_soup(html)
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # ── 提取日期（多策略）────────────────────────────
    article_date = ""
    _date_debug_hits = []
    _now = datetime.now()
    _max_future = _now + timedelta(days=1)
    _chosen = ("", "")  # (source, raw)

    def _accept_dt(d: datetime):
        try:
            return d and (datetime(2000, 1, 1) <= d <= _max_future)
        except Exception:
            return False

    # 优先使用 Playwright 子进程已提取的日期
    if pw_extracted_date:
        _date_debug_hits.append(("pw", pw_extracted_date[:60]))
        d = parse_date(pw_extracted_date)
        if d and _accept_dt(d):
            article_date = d.strftime("%Y-%m-%d")
            _chosen = ("pw", pw_extracted_date[:80])
        else:
            # 保留原始值供调试，但不直接作为最终 date 返回（避免命名回退到今天）
            pw_extracted_date = ""

    # 策略1：meta 标签（如果子进程没拿到才继续）
    if not article_date:
        pass  # 继续下面的策略

    # 策略1：meta 标签
    for meta_name in ["article:published_time", "og:article:published_time",
                      "datePublished", "pubdate", "publish-date", "date",
                      "DC.date.issued", "sailthru.date"]:
        tag = (soup.find("meta", attrs={"property": meta_name}) or
               soup.find("meta", attrs={"name": meta_name}) or
               soup.find("meta", attrs={"itemprop": meta_name}))
        if tag and tag.get("content"):
            _date_debug_hits.append((f"meta:{meta_name}", tag["content"][:60]))
            d = parse_date(tag["content"])
            if d and _accept_dt(d):
                article_date = d.strftime("%Y-%m-%d")
                _chosen = (f"meta:{meta_name}", tag["content"][:80])
                break

    # 策略2：JSON-LD
    if not article_date:
        import json as _json
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = _json.loads(script.string or "")
                if isinstance(data, list): data = data[0]
                for key in ["datePublished","dateCreated","dateModified"]:
                    val = data.get(key,"")
                    if val:
                        _date_debug_hits.append((f"jsonld:{key}", str(val)[:60]))
                        d = parse_date(str(val))
                        if d and _accept_dt(d):
                            article_date = d.strftime("%Y-%m-%d")
                            _chosen = (f"jsonld:{key}", str(val)[:80])
                            break
                if article_date: break
            except Exception: pass

    # 策略3：<time> 标签
    if not article_date:
        for t in soup.find_all("time"):
            raw = t.get("datetime") or t.get_text(strip=True)
            if raw and re.search(r"20\d{2}", raw):
                _date_debug_hits.append(("time", str(raw)[:60]))
            d = parse_date(raw)
            if d and _accept_dt(d):
                article_date = d.strftime("%Y-%m-%d")
                _chosen = ("time", str(raw)[:80])
                break

    # 策略4：含日期关键词的 class/id
    if not article_date:
        for sel in ["[class*='date']","[class*='Date']","[class*='time']",
                    "[class*='published']","[class*='created']","[class*='posted']",
                    "[itemprop='datePublished']","[data-date]",
                    ".article-date",".post-date",".news-date",".pubdate",".timestamp"]:
            try:
                el = soup.select_one(sel)
                if el:
                    raw = el.get("datetime") or el.get("data-date") or el.get_text(strip=True)
                    if raw and re.search(r"20\d{2}", str(raw)):
                        _date_debug_hits.append((f"css:{sel}", str(raw)[:60]))
                    d = parse_date(raw)
                    if d and _accept_dt(d):
                        article_date = d.strftime("%Y-%m-%d")
                        _chosen = (f"css:{sel}", str(raw)[:80])
                        break
            except Exception: pass

    # 策略5：从 URL 路径提取 /YYYY/MM/DD/
    if not article_date:
        m = re.search(r'/(\d{4})[/\-](\d{2})[/\-](\d{2})/', url)
        if m:
            try:
                d = datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)))
                if _accept_dt(d):
                    article_date = d.strftime("%Y-%m-%d")
                    _chosen = ("url", m.group(0))
            except Exception: pass

    # 策略6：正文里搜索日期文字
    if not article_date:
        text_blob = soup.get_text()[:5000]
        patterns = [
            r'(\d{1,2})[.\s](January|February|March|April|May|June|July|August|September|October|November|December)[.\s,]+(20\d{2})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})',
            r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})',
            r'(\d{1,2})[.\-/](\d{1,2})[.\-/](20\d{2})',
        ]
        for pat in patterns:
            m = re.search(pat, text_blob, re.IGNORECASE)
            if m:
                _date_debug_hits.append((f"text:{pat}", m.group(0)[:60]))
                d = parse_date(m.group(0))
                if d and _accept_dt(d) and d.year >= 2020:
                    article_date = d.strftime("%Y-%m-%d")
                    _chosen = (f"text:{pat}", m.group(0)[:80])
                    break

    # 如果还是没拿到日期，输出可读的调试线索（不影响主流程）
    if debug:
        print("         🔎 日期抽样验真：")
        if article_date:
            print(f"           - 采用: {article_date}  来源: {_chosen[0]}  原始: {_chosen[1]}")
        else:
            print("           - 未获得可信发布日期")
        if _date_debug_hits:
            for k, v in _date_debug_hits[:8]:
                print(f"           - 候选: {k}: {v}")
        else:
            print("           - 未发现任何日期候选")
    elif not article_date and _date_debug_hits:
        print("         ⚠ 未解析出日期，已发现的候选：")
        for k, v in _date_debug_hits[:6]:
            print(f"           - {k}: {v}")

    # Roland Berger 特殊：页面常维护更新，dcterms.modified 更贴近“本页展示的最新日期”
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if "rolandberger.com" in host:
        try:
            mt = soup.find("meta", attrs={"name": "dcterms.modified"})
            mod_raw = (mt.get("content") if mt else "") or ""
            mod_dt = parse_date(mod_raw)
        except Exception:
            mod_dt = None
            mod_raw = ""
        pub_dt = parse_date(article_date) if article_date else None
        # 若 modified 更晚且可信（不在未来），用 modified 作为过滤/命名日期
        if mod_dt and _accept_dt(mod_dt) and (pub_dt is None or mod_dt > pub_dt):
            article_date = mod_dt.strftime("%Y-%m-%d")
            if debug:
                print(f"         🔁 使用 dcterms.modified 覆盖发布日期: {article_date} (raw={mod_raw[:30]})")

    skip = ["cookie", "privacy", "linkedin", "facebook", "newsletter",
            "subscribe", "follow us", "terms of use", "all rights reserved",
            "sign up", "register", "log in", "share this", "related articles"]

    # 先找最佳正文容器（article/main/特定class），比 h1.find_all_next 更准确
    body = []
    body_container = None
    for sel in [
        # 语义标签（最优先）
        "article", "[role='main']", "main",
        # Bain 专属
        "[class*='hero-article']", "[class*='article-page']",
        "[class*='snap-chart']", "[class*='insight-content']",
        "[class*='report-content']", "[class*='brief-content']",
        # 通用咨询公司 class 模式
        "[class*='article-body']",   "[class*='article-content']",
        "[class*='article__body']",  "[class*='article__content']",
        "[class*='post-content']",   "[class*='entry-content']",
        "[class*='content-body']",   "[class*='content__body']",
        "[class*='story-body']",     "[class*='news-body']",
        "[class*='press-release']",  "[class*='insight-body']",
        "[class*='page-content']",   "[class*='page__content']",
        "[class*='rich-text']",      "[class*='richtext']",
        "[class*='prose']",          "[class*='body-copy']",
        "[class*='editorial']",      "[class*='text-content']",
    ]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 200:
            body_container = el
            break

    # 如果找到容器就在容器内提取，否则从 h1 往后找
    source = body_container if body_container else (h1 if h1 else soup)
    iterator = (source.find_all(["p","h2","h3","h4","li"])
                if body_container else
                h1.find_all_next(["p","h2","h3","h4","li"]) if h1
                else soup.find_all(["p","h2","h3","h4","li"]))

    for el in iterator:
        text = el.get_text(strip=True)
        if not text or len(text) < 20: continue
        if any(s in text.lower() for s in skip): continue
        body.append({"tag": el.name, "text": text})
        if len(body) >= 80: break

    # 如果还是没有，退而求其次：取所有 <p> 文本
    if not body:
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) < 40: continue
            if any(s in text.lower() for s in skip): continue
            body.append({"tag": "p", "text": text})
            if len(body) >= 50: break

    return title, body, article_date

# ════════════════════════════════════════════════════
# 生成 PDF
# ════════════════════════════════════════════════════
def build_pdf(article, body, font, fpath, screenshot_path=None):
    """
    生成 PDF。
    screenshot_path: 若提供且 body 为空或很少，将截图直接嵌入 PDF，
                     确保图表类页面也有完整内容。
    """
    ACCENT = colors.HexColor("#1a3a5c")
    DARK   = colors.HexColor("#111111")
    GRAY   = colors.HexColor("#555555")
    LGRAY  = colors.HexColor("#cccccc")

    def ps(n, sz=10, c=DARK, lead=None, b=0, a=6):
        kw = dict(fontName=font, fontSize=sz, textColor=c, spaceBefore=b, spaceAfter=a)
        if lead: kw["leading"] = lead
        return ParagraphStyle(n, **kw)

    def esc(t):
        return t.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    def _link_href(href):
        return href.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # A4 可用宽度（mm → points：1mm ≈ 2.835pt）
    PAGE_W = A4[0] - 44*mm   # 左右各 22mm 边距

    doc = SimpleDocTemplate(fpath, pagesize=A4,
        leftMargin=22*mm, rightMargin=22*mm, topMargin=18*mm, bottomMargin=18*mm)

    # ── 判断是否需要嵌入截图 ─────────────────────────────────
    # 不仅看段数，也看正文是否明显偏薄/像 teaser，避免把摘要硬排成“完整 PDF”。
    _body_chars = _body_total_chars(body)
    use_screenshot = (
        screenshot_path
        and os.path.exists(screenshot_path)
        and os.path.getsize(screenshot_path) > 20 * 1024
        and _body_chars < 300  # 正文够用时不嵌截图，保证 PDF 优先是纯文字
    )

    if use_screenshot:
        story = []
        try:
            _iw, _ih = ImageReader(screenshot_path).getSize()
            # 截图型 PDF 把截图放在第一页顶部，避免先渲染标题后把大图挤到后页/留白。
            _frame_h = A4[1] - 36*mm
            _avail_h = _frame_h - 42*mm
            _scale = min(PAGE_W / float(_iw), _avail_h / float(_ih))
            img = RLImage(screenshot_path, width=_iw * _scale, height=_ih * _scale)
            story.append(img)
            story.append(Spacer(1, 8))
            story.append(Paragraph(esc(article.get("title", "")), ps("h1", 15, DARK, lead=20, a=4)))
            story.append(Paragraph(article.get("date","") or "Date unknown", ps("dt", 9, GRAY, a=2)))
            _url = article.get("url", "") or ""
            if _url:
                story.append(Paragraph(
                    f"<a href='{_link_href(_url)}' color='blue'>{esc(_url)}</a>",
                    ps("urlshot", 7, LGRAY, a=8)))
            story.append(Paragraph(
                "Embedded page screenshot. Full text extraction was incomplete, so the captured page is preserved directly.",
                ps("cap", 8, GRAY, a=4)))
        except Exception as img_err:
            story = [
                Paragraph(esc(article.get("title", "")), ps("h1", 17, DARK, lead=23, a=6)),
                HRFlowable(width="100%", thickness=2, color=ACCENT, spaceAfter=8),
                Paragraph(article.get("date","") or "Date unknown", ps("dt", 9, GRAY, a=4)),
                Paragraph(esc(article.get("url","")), ps("url", 7, LGRAY, a=14)),
            ]
            # 图片嵌入失败时降级到文字提示
            story.append(Paragraph(
                f"Full content shown as screenshot. Visit: {article.get('url','')}",
                ps("nb", 10, GRAY)))
    elif not body:
        story = [
            Paragraph(esc(article.get("title", "")), ps("h1", 17, DARK, lead=23, a=6)),
            HRFlowable(width="100%", thickness=2, color=ACCENT, spaceAfter=8),
            Paragraph(article.get("date","") or "Date unknown", ps("dt", 9, GRAY, a=4)),
            Paragraph(esc(article.get("url","")), ps("url", 7, LGRAY, a=14)),
        ]
        story.append(Paragraph(
            f"Full text not retrieved. Visit: {article.get('url','')}",
            ps("nb", 10, GRAY)))
    else:
        story = [
            Paragraph(esc(article.get("title", "")), ps("h1", 17, DARK, lead=23, a=6)),
            HRFlowable(width="100%", thickness=2, color=ACCENT, spaceAfter=8),
            Paragraph(article.get("date","") or "Date unknown", ps("dt", 9, GRAY, a=4)),
            Paragraph(esc(article.get("url","")), ps("url", 7, LGRAY, a=14)),
        ]
        _preview_mode = _body_looks_teaser(article, body) or _body_looks_search_snippets(article, body)
        if _preview_mode:
            pv = _preview_payload(article, body)
            story.append(Paragraph("Preview", ps("hx", 11, ACCENT, lead=15, a=8)))
            for item in (pv.get("blocks") or []):
                raw = item.get("text", "")
                if not raw:
                    continue
                text = esc(raw)
                tag = item.get("tag", "p")
                if tag in ("h2", "h3", "h4"):
                    story.append(Paragraph(text, ps("hx2", 12, DARK, lead=16, b=8, a=4)))
                elif tag == "li":
                    story.append(Paragraph(f"• {text}", ps("li2", 10, DARK, lead=16, a=4)))
                else:
                    story.append(Paragraph(text, ps("p2", 10, DARK, lead=16, a=6)))
            if pv.get("links"):
                story.append(Spacer(1, 6))
                story.append(Paragraph("Related insights", ps("hx3", 11, ACCENT, lead=15, a=6)))
                for lk in pv["links"]:
                    txt = esc(lk.get("text", ""))
                    href = _link_href(lk.get("href", ""))
                    if txt and href:
                        story.append(Paragraph(
                            f"• <a href='{href}' color='blue'>{txt}</a>",
                            ps("li3", 9, DARK, lead=14, a=4)))
            story.append(Spacer(1, 4))
            story.append(Paragraph(pv["note"], ps("nb2", 8, GRAY, lead=12, a=2)))
        else:
            for item in body:
                if not isinstance(item, dict):
                    continue
                tag  = item.get("tag", "p") or "p"
                raw  = item.get("text", "")
                if not isinstance(raw, str) or not raw.strip():
                    continue
                text = esc(raw)
                if tag in ("h2","h3","h4"):
                    story.append(Paragraph(text, ps("hx",12,DARK,lead=16,b=8,a=4)))
                elif tag == "li":
                    story.append(Paragraph(f"• {text}", ps("li",10,DARK,lead=16,a=4)))
                else:
                    story.append(Paragraph(text, ps("p",10,DARK,lead=16,a=6)))

    story += [
        Spacer(1, 16),
        HRFlowable(width="100%", thickness=0.5, color=LGRAY, spaceAfter=4),
        Paragraph(
            f"Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M')}  ·  {article.get('url','')}",
            ps("ft", 7, LGRAY, a=0))
    ]
    doc.build(story)

# ════════════════════════════════════════════════════
# 截图：直接渲染文章正文 HTML，完全绕开弹窗
# ════════════════════════════════════════════════════
def build_article_html(article, body):
    """把文章正文组装成干净的 HTML 页面供截图"""
    import html as _html_esc
    title = _html_esc.unescape(article.get("title", ""))
    date  = article.get("date", "")
    url   = article.get("url", "")
    _body_items = _body_text_items(body)
    _teaser_body = _body_looks_teaser(article, body) or _body_looks_search_snippets(article, body)
    render_body = [] if _teaser_body else _body_items

    _is_preview = bool(_teaser_body) or not render_body
    _api_body_raw = (article.get("_api_body_html") or "").strip()
    _summary_text = _html_esc.unescape((article.get("summary") or "").strip())
    _links = _preview_links(article) if _api_body_raw else []
    _feed_cards = _mck_theme_feed_cards(article, limit=8)

    paragraphs = ""
    for item in render_body:
        text = str(item["text"]).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        tag  = item.get("tag", "p")
        if tag in ("h2","h3","h4"):
            paragraphs += f"<{tag} style='margin:20px 0 8px;color:#1a3a5c'>{text}</{tag}>"
        elif tag == "li":
            paragraphs += f"<li style='margin:4px 0'>{text}</li>"
        else:
            paragraphs += f"<p style='margin:0 0 14px;line-height:1.7'>{text}</p>"

    if not paragraphs:
        _api_blocks = _extract_blocks_from_html_fragment(_api_body_raw) if _api_body_raw else []
        _preview_blocks = _api_blocks or _best_effort_preview_blocks(article)
        if not _preview_blocks and _summary_text:
            _preview_blocks = [{"tag": "p", "text": _summary_text}]
        for _it in _preview_blocks[:12]:
            _safe = _html_esc.escape(_it.get("text", ""))
            _tag = _it.get("tag", "p")
            if _tag in ("h2", "h3", "h4"):
                paragraphs += f"<{_tag} style='margin:20px 0 8px;color:#1a3a5c'>{_safe}</{_tag}>"
            elif _tag == "li":
                paragraphs += f"<li style='margin:4px 0'>{_safe}</li>"
            else:
                paragraphs += f"<p style='margin:0 0 14px;line-height:1.7'>{_safe}</p>"
        if not paragraphs:
            paragraphs = (
                f"<p style='line-height:1.7;color:#555'>Content preview is not available. "
                f"Visit the article at <a href='{url}' style='color:#1a3a5c'>{_html_esc.escape(url)}</a></p>"
            )

    from datetime import datetime as _dt
    from urllib.parse import urlparse as _up
    company = _up(url).netloc.replace("www.","").split(".")[0].capitalize()
    now_str = _dt.now().strftime("%Y-%m-%d %H:%M")

    links_html = ""
    _cards = []
    _seen_cards = set()
    for _lk in (_links + _feed_cards):
        _k = ((_lk.get("href") or "").strip().lower(), (_lk.get("text") or "").strip().lower())
        if not _k[0] or _k in _seen_cards:
            continue
        _seen_cards.add(_k)
        _cards.append(_lk)
        if len(_cards) >= 8:
            break
    if _cards:
        links_items = ""
        for _lk in _cards:
            _txt = _html_esc.escape(_lk.get("text", ""))
            _href = _html_esc.escape(_lk.get("href", ""))
            _desc = _html_esc.escape(_lk.get("desc", ""))
            _date = _html_esc.escape(_lk.get("date", ""))
            if _txt and _href:
                _meta = f"<span class=\"link-meta\">{_date}</span>" if _date else ""
                _desc_html = f"<span class=\"link-desc\">{_desc}</span>" if _desc else ""
                links_items += f"""<a href="{_href}" class="link-card">
                    <span class="link-arrow">&#8594;</span>
                    <span class="link-text-wrap">
                        <span class="link-text">{_txt}</span>
                        {_meta}
                        {_desc_html}
                    </span>
                </a>"""
        if links_items:
            links_html = f"""<div class="related-section">
                <div class="related-header">Related insights</div>
                <div class="related-grid">{links_items}</div>
            </div>"""

    preview_badge = ""
    if _is_preview:
        _domain_display = _html_esc.escape(url.split("/")[2]) if url.startswith("http") else company + ".com"
        preview_badge = f"""<div class="preview-badge">
            <span class="badge-icon">&#128274;</span>
            <span><strong>正文受访问限制，仅显示摘要</strong><br>
            完整内容请访问原文：
            <a href="{_html_esc.escape(url)}" style="color:#1a3a5c;text-decoration:underline;word-break:break-all">{_html_esc.escape(url)}</a>
            </span>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
html, body {{ background: #ffffff; height: auto; min-height: 0; }}
body {{
    color: #1a1a1a;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    font-size: 15px;
    line-height: 1.7;
    width: 860px;
    padding: 32px 48px 28px;
    margin: 0 auto;
}}
.topbar {{ height: 5px; background: linear-gradient(90deg, #1a3a5c, #2a6496); margin-bottom: 22px; }}
.label {{
    font-size: 11px; font-weight: 700; letter-spacing: 0.15em;
    text-transform: uppercase; color: #1a3a5c; margin-bottom: 14px;
}}
h1 {{
    font-size: 24px; font-weight: 800; line-height: 1.3;
    color: #0d0d0d; margin-bottom: 10px; letter-spacing: -0.01em;
}}
.meta {{
    font-size: 12px; color: #888; padding-bottom: 14px;
    border-bottom: 1px solid #ddd; margin-bottom: 18px;
}}
.meta a {{ color: #1a3a5c; text-decoration: none; }}
.body {{ color: #2a2a2a; }}
.body p {{ margin-bottom: 14px; }}
.body h2, .body h3 {{ font-size: 16px; font-weight: 700; color: #1a3a5c; margin: 20px 0 8px; }}
.body li {{ margin: 0 0 6px 20px; list-style: disc; }}
.preview-badge {{
    display: flex; align-items: flex-start; gap: 10px;
    margin-top: 18px; padding: 14px 18px;
    background: #fafbfc; border-left: 4px solid #b0bec5;
    border-radius: 0 6px 6px 0; font-size: 12px; color: #555;
    line-height: 1.6;
}}
.badge-icon {{ font-size: 18px; flex-shrink: 0; margin-top: 1px; }}
.related-section {{
    margin-top: 20px; padding: 18px 20px;
    border: 1px solid #e2e8f0; border-radius: 10px; background: #f8fafc;
}}
.related-header {{
    font-size: 13px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; color: #1a3a5c; margin-bottom: 12px;
    padding-bottom: 8px; border-bottom: 2px solid #e2e8f0;
}}
.related-grid {{ display: flex; flex-direction: column; gap: 6px; }}
.link-card {{
    display: flex; align-items: center; gap: 10px;
    padding: 10px 14px; background: #fff; border: 1px solid #e8ecf1;
    border-radius: 8px; text-decoration: none; color: #1a3a5c;
    font-size: 13px; font-weight: 600; line-height: 1.4;
    transition: background 0.15s;
}}
.link-card:hover {{ background: #edf2f7; }}
.link-arrow {{ color: #2a6496; font-size: 16px; flex-shrink: 0; }}
.link-text {{ flex: 1; }}
.footer {{
    margin-top: 20px; padding-top: 10px;
    border-top: 1px solid #ececec; font-size: 10px; color: #bbb;
}}
img {{ display: none !important; }}
</style>
</head>
<body>
<div class="topbar"></div>
<div class="label">{_html_esc.escape(company)} &nbsp;&middot;&nbsp; Press Release</div>
<h1>{_html_esc.escape(title)}</h1>
<div class="meta">
    <strong>{date or "Date unknown"}</strong>
    &nbsp;&nbsp;|&nbsp;&nbsp;
    <a href="{_html_esc.escape(url)}">{_html_esc.escape(url[:90])}{"..." if len(url)>90 else ""}</a>
</div>
<div class="body">{paragraphs}</div>
{preview_badge}
{links_html}
<div class="footer">Downloaded {now_str} &nbsp;&middot;&nbsp; {_html_esc.escape(url)}</div>
</body>
</html>"""


async def take_screenshot(url, fpath, article=None, body=None, _shared_page=None):
    """
    截图策略：
      1. patchright（优先）或 playwright 打开真实网页截图
      2. 截图为空白或 CF 验证页 → Archive.org 存档截图
      3. 全部失败 → 本地渲染 HTML 截图（降级兜底）
    """

    # ── 本地 HTML 渲染（降级兜底）──────────────────────────
    async def _local_html_screenshot(pg=None):
        if article is None:
            return False, "no article"
        html_content = build_article_html(article, body or [])
        async def _render(page):
            await page.set_viewport_size({"width": 860, "height": 100})
            await page.set_content(html_content, wait_until="domcontentloaded")
            await page.add_style_tag(content="html,body{background:#fff!important;color:#1a1a1a!important;height:auto!important;min-height:0!important}img{display:none!important}")
            await page.wait_for_timeout(200)
            await page.screenshot(path=fpath, full_page=True, type="png", animations="disabled")
        if pg is not None:
            try:
                await _render(pg)
                return True, "local_html"
            except Exception as e:
                return False, str(e)
        else:
            async with async_playwright() as p:
                b = await p.chromium.launch(headless=True, args=["--no-sandbox","--force-color-profile=srgb"])
                c = await b.new_context(
                    viewport={"width": 860, "height": 100},
                    color_scheme="light")
                pg2 = await c.new_page()
                try:
                    await _render(pg2)
                    await b.close()
                    return True, "local_html"
                except Exception as e:
                    await b.close()
                    return False, str(e)

    # ── 像素采样判断空白 ────────────────────────────────────
    def _is_blank(path, threshold=0.95):
        import struct, zlib as _z
        try:
            with open(path,"rb") as f: data=f.read()
            if data[:8]!=b"\x89PNG\r\n\x1a\n": return False
            w=struct.unpack(">I",data[16:20])[0]; h=struct.unpack(">I",data[20:24])[0]
            ct=data[25]
            if data[24]!=8 or ct not in (2,6): return False
            idat=bytearray(); pos=8
            while pos<len(data)-12:
                n=struct.unpack(">I",data[pos:pos+4])[0]
                if data[pos+4:pos+8]==b"IDAT": idat+=data[pos+8:pos+8+n]
                elif data[pos+4:pos+8]==b"IEND": break
                pos+=12+n
            raw=_z.decompress(bytes(idat)); bpp=4 if ct==6 else 3; stride=1+w*bpp
            white=total=0
            for ri in range(0,h,max(1,h//40)):
                rs=ri*stride+1
                for ci in range(0,w,max(1,w//50)):
                    ps=rs+ci*bpp
                    if ps+bpp>len(raw): continue
                    r,g,b_=raw[ps],raw[ps+1],raw[ps+2]; total+=1
                    if r>=230 and g>=230 and b_>=230: white+=1
            return (white/total)>=threshold if total else False
        except Exception: return False

    # Porsche Consulting：去掉 URL 中的锚点（#ov 等会触发 Filter 遮罩弹窗）
    if "porsche-consulting.com" in url and "#" in url:
        url = url.split("#")[0]

    # ══════════════════════════════════════════════════════
    # CF 域名：ScrapingBee 已停用，走 patchright headless=False
    # ══════════════════════════════════════════════════════
    if any(d in url for d in CF_TURNSTILE_DOMAINS):
        if False and SCRAPINGBEE_KEY:  # 已停用
            try:
                import urllib.parse as _up, json as _json, requests as _rq
                _pg_domain = urlparse(url).netloc

                # 策略A：预设 OneTrust cookie，让弹窗根本不出现（最稳）
                _ot_cookie = (
                    "OptanonAlertBoxClosed=2024-01-01T00:00:00.000Z; "
                    "OptanonConsent=isGpcEnabled=0&datestamp=Mon+Jan+01+2024&version=202401.2.0"
                    "&browserGpcFlag=0&isIABGlobal=false&consentId=fixed&interactionCount=2"
                    "&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1; "
                    "CookieConsent=true"
                )
                # 策略B：js_scenario 等页面稳定后强制隐藏所有弹窗
                _js_scenario = {"instructions": [
                    {"wait": 5000},
                    {"evaluate": """(function(){
                        // 点击 OneTrust 接受按钮
                        ['#onetrust-accept-btn-handler','#accept-recommended-btn-handler',
                         '.onetrust-close-btn-handler','button[id*=onetrust]',
                         'button[class*=acceptAll]','button[class*=accept-all]',
                         'button[data-accept-all]'
                        ].forEach(function(s){
                            var el=document.querySelector(s);
                            if(el)el.click();
                        });
                        // 强制移除所有遮罩/弹窗
                        document.querySelectorAll(
                            '#onetrust-consent-sdk,#onetrust-banner-sdk,'
                            +'#onetrust-pc-sdk,.onetrust-pc-dark-filter,'
                            +'[id*=cookie],[id*=consent],[id*=gdpr],'
                            +'[class*=cookie-banner],[class*=consent-banner],'
                            +'[class*=modal-overlay],[class*=popup],[role=dialog]'
                        ).forEach(function(el){
                            el.style.setProperty('display','none','important');
                        });
                        document.body.style.setProperty('overflow','auto','important');
                    })()"""},
                    {"wait": 1500}
                ]}
                _sb_params = {
                    "api_key":              SCRAPINGBEE_KEY,
                    "url":                  url,
                    "render_js":            "true",
                    "premium_proxy":        "true",
                    "screenshot":           "true",
                    "screenshot_full_page": "true",
                    "block_ads":            "true",
                    "window_width":         "1280",
                    "window_height":        "900",
                    "cookies":              _ot_cookie,
                    "js_scenario":          _json.dumps(_js_scenario),
                }
                print(f"         → ScrapingBee 截图（预设Cookie+强制隐藏弹窗）...")

                # ScrapingBee 截图：curl_cffi GET + 预设 cookie + js_scenario POST 关闭弹窗
                import json as _json2, urllib.parse as _up3
                _sb_api = "https://app.scrapingbee.com/api/v1/"

                # 测试验证：图3/图4 用 js_scenario 能成功关闭弹窗
                # 关键：用 requests 库而非 curl_cffi 发请求（测试脚本里 requests 成功，curl_cffi 报500）
                import json as _json2, urllib.parse as _up3
                _js_scen = {"instructions": [
                    {"wait": 4000},
                    {"evaluate": "document.querySelectorAll('#onetrust-consent-sdk,#onetrust-banner-sdk,[id*=cookie],[class*=cookie],[class*=consent],[role=dialog],[class*=overlay]').forEach(e=>e.remove());var b=document.querySelector('#onetrust-accept-btn-handler');if(b)b.click();"},
                    {"wait": 1000}
                ]}
                _sb_params_ss = {
                    "api_key":              SCRAPINGBEE_KEY,
                    "url":                  url,
                    "render_js":            "true",
                    "premium_proxy":        "true",
                    "screenshot":           "true",
                    "screenshot_full_page": "true",
                    "block_ads":            "true",
                    "window_width":         "1280",
                    "window_height":        "900",
                    "cookies":              "OptanonAlertBoxClosed=2024-01-01T00%3A00%3A00.000Z",
                    "js_scenario":          _json2.dumps(_js_scen),
                }
                _sb_r = None
                try:
                    # 用标准 requests 库（测试脚本验证有效，curl_cffi 会报500）
                    import requests as _rq_std
                    _sb_r = _rq_std.get(_sb_api, params=_sb_params_ss, timeout=90)
                    if _sb_r and _sb_r.status_code != 200:
                        print(f"         → js_scenario失败(HTTP {_sb_r.status_code})，降级无js版本...")
                        _sb_params_fallback = {k:v for k,v in _sb_params_ss.items() if k != "js_scenario"}
                        _sb_params_fallback["wait"] = "8000"
                        _sb_r = _rq_std.get(_sb_api, params=_sb_params_fallback, timeout=90)
                except Exception as _ce:
                    print(f"         → requests截图异常: {_ce}")

                # 检测 PNG 文件头（\x89PNG），不依赖 content-type 或 status_code
                # 因为 requests + LibreSSL 环境下 headers 可能读取异常
                _is_png = (_sb_r and len(_sb_r.content) > 100
                           and _sb_r.content[:4] == b"\x89PNG")
                if _is_png:
                    with open(fpath, "wb") as _f:
                        _f.write(_sb_r.content)
                    kb = len(_sb_r.content)//1024
                    print(f"         → ScrapingBee 截图成功（{kb}KB）")
                    return True, "scrapingbee", ""
                else:
                    code = _sb_r.status_code if _sb_r else 0
                    ct = _sb_r.headers.get("content-type","") if _sb_r else ""
                    body_err = (getattr(_sb_r,"text","") or "")[:150]
                    print(f"         → ScrapingBee 截图失败(HTTP {code} ct={ct}): {body_err}")
            except Exception as _sbe:
                print(f"         → ScrapingBee 截图异常: {_sbe}")

        # ScrapingBee 失败或未配置：本地 HTML 渲染
        print(f"         → CF Turnstile 域名，本地渲染截图")
        ok, msg = await _local_html_screenshot(_shared_page)
        return (True, "cf_local", "") if ok else (False, f"cf_local:{msg}", "")

    # ══════════════════════════════════════════════════════
    # CF / HTTP2 硬拦截域名：直连必失败，直跳 Archive.org
    # morganstanley.com: Playwright ERR_HTTP2_PROTOCOL_ERROR
    # ══════════════════════════════════════════════════════
    _SS_CF_HARDBLOCK = ["bain.com", "bcg.com", "morganstanley.com"]
    if any(d in url for d in _SS_CF_HARDBLOCK):
        print(f"         → 硬拦截域名，跳过直连，尝试 Archive.org → 本地 HTML")

        # 策略A: Archive.org
        try:
            def _query_archive():
                try:
                    import requests as _rq
                    _hdr = {"User-Agent": "Googlebot/2.1"}
                    r = _rq.get(f"https://archive.org/wayback/available?url={url}",
                                timeout=10, headers=_hdr)
                    if r.status_code == 200:
                        snap = r.json().get("archived_snapshots", {}).get("closest", {})
                        if snap.get("status") == "200":
                            return snap["url"]
                except Exception:
                    pass
                try:
                    import requests as _rq
                    _direct = f"https://web.archive.org/web/2026/{url}"
                    _dr = _rq.head(_direct, timeout=10, allow_redirects=True,
                                   headers={"User-Agent": "Mozilla/5.0"})
                    if _dr.status_code == 200:
                        return _dr.url
                except Exception:
                    pass
                return ""
            _loop = asyncio.get_running_loop()
            arch_url = await _loop.run_in_executor(None, _query_archive)
            if arch_url:
                async with async_playwright() as p:
                    b  = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                    c  = await b.new_context(viewport={"width": 1280, "height": 900})
                    pg = await c.new_page()
                    await pg.goto(arch_url, wait_until="domcontentloaded", timeout=45000)
                    await pg.wait_for_timeout(3000)
                    await pg.evaluate("""()=>{
                        ['#wm-ipp-base','#wm-ipp','#donato','#wm-ipp-print',
                         '.wb-autocomplete-suggestions','#wm-btns'].forEach(s=>{
                            const e=document.querySelector(s);
                            if(e) e.style.display='none';
                        });
                        document.body.style.marginTop='0';
                    }""")
                    arch_html = await pg.content()
                    await pg.screenshot(path=fpath, full_page=True, type="png")
                    await b.close()
                _arch_ok = not _is_blank(fpath)
                if _arch_ok and "mckinsey.com" in url:
                    _arch_ok, _arch_reason = _mck_page_quality_ok(url, arch_html, article)
                    if not _arch_ok:
                        print(f"         ⚠ Archive.org 页面质量不足（{_arch_reason}）")
                if _arch_ok:
                    print(f"         ✅ Archive.org 截图成功")
                    return True, "archive.org", arch_html
                else:
                    print(f"         ⚠ Archive.org 截图为空白")
            else:
                print(f"         → Archive.org 无快照")
        except Exception as _ae:
            print(f"         → Archive.org 截图失败: {_ae}")

        print(f"         → 降级本地 HTML 渲染")
        ok, msg = await _local_html_screenshot(_shared_page)
        if ok:
            return True, "cf_local_html", ""
        return False, f"cf_all_failed:{msg}", ""

    # ══════════════════════════════════════════════════════
    # 策略1：真实网页截图（patchright 优先，headless=True）
    # ══════════════════════════════════════════════════════
    real_ok   = False
    real_html = ""
    try:
        if PATCHRIGHT_AVAILABLE:
            from patchright.async_api import async_playwright as _apw
        else:
            from playwright.async_api import async_playwright as _apw

        # Cloudflare 已知域名列表：headless=True 会被识别，必须用 headless=False
        _CF_DOMAINS = [
            "mckinsey.com", "kearney.com", "accenture.com", "deloitte.com",
            "pwc.com", "kpmg.com",
        ]
        _use_headed = any(d in url for d in _CF_DOMAINS)

        async with _apw() as p:
            browser = await p.chromium.launch(
                headless=not _use_headed,   # CF 域名用有头模式绕过检测
                args=["--no-sandbox","--disable-dev-shm-usage",
                      "--force-color-profile=srgb",
                      "--disable-blink-features=AutomationControlled",
                      "--start-maximized"],
                slow_mo=50 if _use_headed else 0,   # 有头模式加一点延迟更像真人
            )
            ctx = await browser.new_context(
                viewport={"width":1280,"height":900},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                locale="en-US",
                java_script_enabled=True,
            )
            try:
                domain = urlparse(url).netloc
                _cookies = [
                    {"name":"CookieConsent",         "value":"true",     "domain":domain, "path":"/"},
                    {"name":"OptanonAlertBoxClosed",  "value":"true",     "domain":domain, "path":"/"},
                    {"name":"cookieConsent",          "value":"accepted", "domain":domain, "path":"/"},
                    {"name":"notice_gdpr_prefs",      "value":"0|1|2",   "domain":domain, "path":"/"},
                    {"name":"osano_consentmanager",   "value":"%7B%22ESSENTIAL%22%3A%22ACCEPT%22%2C%22MARKETING%22%3A%22ACCEPT%22%2C%22PERSONALIZATION%22%3A%22ACCEPT%22%2C%22ANALYTICS%22%3A%22ACCEPT%22%2C%22OPT_OUT%22%3A%22ACCEPT%22%7D", "domain":domain, "path":"/"},
                    {"name":"osano_consentmanager_uuid", "value":"auto-accepted", "domain":domain, "path":"/"},
                ]
                # Porsche Consulting 用 Usercentrics，预设同意 cookie
                if "porsche-consulting.com" in url:
                    _cookies += [
                        {"name":"uc_user_interaction", "value":"true",   "domain":domain, "path":"/"},
                        {"name":"uc_settings",         "value":"eyJkc3Rfb3B0aW4iOnRydWV9", "domain":domain, "path":"/"},
                        {"name":"usercentrics",        "value":"accepted","domain":domain, "path":"/"},
                    ]
                await ctx.add_cookies(_cookies)
            except Exception: pass

            page = await ctx.new_page()
            _is_porsche = "porsche-consulting.com" in url
            await page.add_init_script("""
                Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                window.chrome={runtime:{},loadTimes:function(){},csi:function(){},app:{}};
                Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
                Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
                // Pre-suppress Osano consent manager to prevent overlay
                try {
                    const style = document.createElement('style');
                    style.textContent = '.osano-cm-widget,.osano-cm-dialog,.osano-cm-window,.osano-cm-overlay,.osano-cm-drawer,.osano-cm-info,.osano-cm-save,.osano-cm-storage,[class*="osano"]{display:none!important;visibility:hidden!important;opacity:0!important;pointer-events:none!important}';
                    (document.head || document.documentElement).appendChild(style);
                    const observer = new MutationObserver(()=>{
                        document.querySelectorAll('[class*="osano"]').forEach(el=>{
                            el.style.setProperty('display','none','important');
                        });
                        document.body && (document.body.style.overflow='auto');
                    });
                    observer.observe(document.documentElement, {childList:true, subtree:true});
                } catch(e){}
            """)
            if _is_porsche:
                await page.add_init_script("""
                    // Porsche Consulting: 劫持 IntersectionObserver，让所有元素立刻
                    // 被视为"已进入视口"，从而触发入场动画 class 添加。
                    // 这必须在页面 JS 之前运行，否则 IO 回调会把 opacity 设回 0。
                    (function(){
                        const _RealIO = window.IntersectionObserver;
                        window.IntersectionObserver = function(cb, opts) {
                            const inst = new _RealIO(function(entries, obs){
                                entries.forEach(e => {
                                    Object.defineProperty(e, 'isIntersecting', {value: true});
                                    Object.defineProperty(e, 'intersectionRatio', {value: 1.0});
                                });
                                cb(entries, obs);
                            }, opts);
                            return inst;
                        };
                        window.IntersectionObserver.prototype = _RealIO.prototype;
                        // 全局禁用 CSS 动画和过渡
                        const s = document.createElement('style');
                        s.textContent = '*, *::before, *::after { animation-duration:0s!important; animation-delay:0s!important; transition-duration:0s!important; transition-delay:0s!important; }';
                        (document.head || document.documentElement).appendChild(s);
                        // MutationObserver: 任何新增元素也强制可见
                        const mo = new MutationObserver(()=>{
                            document.querySelectorAll('.js-animItem,.js-anim,[class*="anim"],[class*="fade"],[class*="reveal"]').forEach(el=>{
                                el.style.opacity='1';
                                el.style.visibility='visible';
                                el.style.transform='none';
                            });
                        });
                        if(document.documentElement) mo.observe(document.documentElement, {childList:true, subtree:true});
                    })();
                """)
            # Block cookie consent scripts (Osano, OneTrust etc.) to prevent overlays
            async def _block_consent_scripts(route):
                await route.abort()
            for _pat in ["**/*osano*", "**/*cookie-consent*", "**/*cookieconsent*"]:
                try:
                    await page.route(_pat, _block_consent_scripts)
                except Exception:
                    pass
            try:
                wait_ev = "networkidle" if any(d in url for d in ["spglobal.com","woodmac.com","porsche-consulting.com"]) else "domcontentloaded"
                _resp = await page.goto(url, wait_until=wait_ev, timeout=45000)
            except Exception:
                _resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # ── 快速 404 / 迁移页检测 → 直接走本地 HTML 降级 ─────────
            _page_status = _resp.status if _resp else 0
            if _page_status in (404, 410):
                _page_title = await page.title()
                print(f"         ⚠ 页面返回 HTTP {_page_status}（{_page_title[:40]}），跳过网页截图")
                await browser.close()
                reason = f"http{_page_status}"
                print(f"         ⚠ 截图降级本地HTML(reason={reason})")
                ok, msg = await _local_html_screenshot()
                if ok:
                    return True, f"fallback({reason})", ""
                return False, f"real={reason},fallback={msg}", ""

            # ── CF / Akamai 验证等待（最多 30 秒，轮询检测）────────
            if _use_headed:
                _cf_passed = False
                _akamai_blocked = False
                for _cf_wait in range(30):
                    await page.wait_for_timeout(1000)
                    _cf_title   = await page.title()
                    _cf_content = await page.content()
                    # Akamai "Access Denied" 硬封锁（不同于 CF 挑战，无法通过等待解决）
                    if "Access Denied" in _cf_title or "Access Denied" in _cf_content[:2000]:
                        _akamai_blocked = True
                        print(f"         ⚠ Akamai 封锁（Access Denied），第{_cf_wait+1}秒检测到")
                        break
                    _still_cf   = any(x in (_cf_title + _cf_content) for x in [
                        "Just a moment", "Performing security verification",
                        "Checking your browser", "cf-challenge",
                        "Enable JavaScript and cookies to continue",
                        "DDoS protection",
                    ])
                    if not _still_cf:
                        print(f"         ✓ CF验证通过（等待{_cf_wait+1}秒）")
                        _cf_passed = True
                        break
                if not _cf_passed and not _akamai_blocked:
                    print(f"         ⚠ CF验证等待超时（30秒），继续尝试截图")

                # Akamai 封锁：关闭当前浏览器，降级到 Archive.org / 本地 HTML
                if _akamai_blocked:
                    await browser.close()
                    print(f"         → Akamai 封锁，尝试 Archive.org 降级…")
                    # 策略: Archive.org → 本地 HTML
                    try:
                        import urllib.parse as _up_ak, requests as _rq_ak2
                        _ak_hdr = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
                        _ak_loop = asyncio.get_running_loop()
                        def _query_archive_ak():
                            try:
                                _ak_api = f"https://archive.org/wayback/available?url={_up_ak.quote(url, safe='')}"
                                r = _rq_ak2.get(_ak_api, timeout=15, headers=_ak_hdr)
                                if r.status_code == 200:
                                    snap = r.json().get("archived_snapshots",{}).get("closest",{})
                                    if snap.get("available"):
                                        return snap["url"].replace("http://","https://")
                            except Exception:
                                pass
                            try:
                                _direct = f"https://web.archive.org/web/2026/{url}"
                                _dr = _rq_ak2.head(_direct, timeout=12, allow_redirects=True, headers=_ak_hdr)
                                if _dr.status_code == 200 and "web.archive.org" in _dr.url:
                                    return _dr.url
                            except Exception:
                                pass
                            return ""
                        arch_url = await _ak_loop.run_in_executor(None, _query_archive_ak)
                        if arch_url:
                            # ── 校验快照日期是否足够新 ──────────────────────
                            # 格式: https://web.archive.org/web/20260219010412/https://...
                            import re as _re_ak
                            _snap_ts_m = _re_ak.search(r'/web/(\d{8})\d*/', arch_url)
                            _snap_date_ok = True
                            if _snap_ts_m:
                                try:
                                    from datetime import datetime as _dt_ak
                                    _snap_dt = _dt_ak.strptime(_snap_ts_m.group(1), "%Y%m%d")
                                    _art_date_str = (article or {}).get("date", "") or ""
                                    if _art_date_str:
                                        _art_dt = _dt_ak.strptime(_art_date_str[:10], "%Y-%m-%d")
                                        _days_gap = (_art_dt - _snap_dt).days
                                        if _days_gap > 60:
                                            print(f"         ⚠ Archive.org 快照({_snap_ts_m.group(1)})比文章发布({_art_date_str[:10]})早{_days_gap}天，跳过")
                                            _snap_date_ok = False
                                except Exception:
                                    pass
                            if not _snap_date_ok:
                                print(f"         → 快照过旧，降级到本地 HTML 截图")
                                ok, msg = await _local_html_screenshot()
                                if ok:
                                    return True, "fallback(akamai_stale)", ""
                                return False, "akamai_stale_snap", ""
                            print(f"         → Archive.org: {arch_url[:80]}")
                            async with async_playwright() as _ak_p:
                                _ak_b = await _ak_p.chromium.launch(headless=True, args=["--no-sandbox"])
                                _ak_ctx = await _ak_b.new_context(viewport={"width":860,"height":1200})
                                _ak_pg = await _ak_ctx.new_page()
                                try:
                                    await _ak_pg.goto(arch_url, wait_until="domcontentloaded", timeout=45000)
                                    await _ak_pg.wait_for_timeout(2000)
                                    # 隐藏 Wayback 工具栏
                                    await _ak_pg.evaluate("try{document.querySelector('#wm-ipp-base')?.remove();document.querySelector('#wm-ipp')?.remove();}catch(e){}")
                                    await _ak_pg.screenshot(path=fpath, full_page=True, type="png")
                                    real_html = await _ak_pg.content()
                                    await _ak_b.close()
                                    return True, "archive.org(akamai)", real_html
                                except Exception as _ak_e:
                                    await _ak_b.close()
                                    print(f"         ⚠ Archive.org 截图失败: {_ak_e}")
                    except Exception as _ak_e2:
                        print(f"         ⚠ Archive.org 查询失败: {_ak_e2}")
                    # Archive.org 也失败 → 本地 HTML 降级
                    print(f"         → 降级到本地 HTML 截图")
                    ok, msg = await _local_html_screenshot()
                    if ok:
                        return True, "fallback(akamai)", ""
                    return False, "akamai_blocked", ""

                # ── 等待页面正文内容真正渲染（核心修复）────────────────
                # 各域名用不同的"内容已加载"选择器
                _content_selectors = {
                    "oliverwyman.com": [
                        "article",
                        "main p",
                        "[class*='article']",
                        "[class*='insight']",
                        "[class*='content']",
                        "h1",
                    ],
                    "porsche-consulting.com": [".js-animItem", "article", "main"],
                    "bain.com":      ["article", "main", ".article-body"],
                    "mckinsey.com":  ["article", "main", ".mdc-article"],
                    "bcg.com":       ["article", "main", ".article"],
                    "kearney.com":   ["article", ".article", "main"],
                    "accenture.com": ["article", "main"],
                    "deloitte.com":  ["article", "main"],
                    "pwc.com":       ["article", "main"],
                    "kpmg.com":      ["article", "main"],
                }
                _domain_key = next(
                    (d for d in _content_selectors if d in url), None
                )
                _sels_to_try = (_content_selectors[_domain_key]
                                if _domain_key else ["article", "main", "h1"])

                if "mckinsey.com" in url and "/featured-insights/themes/" in url:
                    _sels_to_try = [
                        "main h1", "article h1", "main p", "article p",
                        "[class*='ArticleBody']", "[class*='article-body']",
                        "[class*='RichText']", "main",
                    ]

                _content_found = False
                for _csel in _sels_to_try:
                    try:
                        await page.wait_for_selector(_csel, timeout=12000,
                                                     state="visible")
                        print(f"         ✓ 正文内容已渲染（{_csel}）")
                        _content_found = True
                        break
                    except Exception:
                        pass
                if not _content_found:
                    print(f"         ⚠ 正文选择器未出现，追加等待 5 秒...")
                    await page.wait_for_timeout(5000)

                # 等 networkidle（资源加载完毕）
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    await page.wait_for_timeout(2000)

                await page.wait_for_timeout(1500)
            else:
                # headless 模式：等正文选择器
                _hl_sels = (
                    [".js-animItem", "article", "main", "h1"]
                    if _is_porsche
                    else ["article", "main", "h1", ".content", "[class*='article']", "[class*='insight']"]
                )
                for _hsel in _hl_sels:
                    try:
                        await page.wait_for_selector(_hsel, timeout=10000,
                                                     state="visible")
                        print(f"         ✓ headless 正文已渲染（{_hsel}）")
                        break
                    except Exception:
                        pass
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass

                # CSS 加载验证：若 stylesheet 数 < 3，说明 CSS 没加载完，重新加载
                if _is_porsche:
                    _css_sheets = await page.evaluate("document.styleSheets.length")
                    if _css_sheets < 3:
                        print(f"         ⚠ CSS 未加载完（{_css_sheets} sheets），reload…")
                        try:
                            await page.reload(wait_until="networkidle", timeout=30000)
                        except Exception:
                            await page.reload(wait_until="domcontentloaded", timeout=20000)
                        await page.wait_for_timeout(2000)
                        _css_sheets = await page.evaluate("document.styleSheets.length")
                        print(f"         → reload 后 CSS sheets: {_css_sheets}")

                    # 核心修复：用 add_style_tag 注入 !important CSS 覆盖动画隐藏
                    # Porsche 的 .js-animItem 初始 opacity:0 由 CSS class 设置，
                    # inline style 无法覆盖（可能有 !important），必须用 stylesheet 级 !important
                    await page.add_style_tag(content="""
                        .js-animItem, .js-anim, [class*="is-animated"],
                        [class*="anim-"], [class*="animItem"],
                        [class*="textIntro"], [class*="textImage"],
                        [class*="textBlock"], [class*="imageBlock"],
                        [class*="quoteBlock"], [class*="stage"],
                        [class*="teaserGrid"], [class*="contentBlock"],
                        [class*="fade"], [class*="slide"], [class*="reveal"],
                        [data-animate], [data-aos],
                        section > div, article > div, main > div {
                            opacity: 1 !important;
                            visibility: visible !important;
                            transform: none !important;
                            clip-path: none !important;
                            -webkit-clip-path: none !important;
                        }
                        *, *::before, *::after {
                            animation-duration: 0s !important;
                            animation-delay: 0s !important;
                            transition-duration: 0s !important;
                            transition-delay: 0s !important;
                        }
                    """)
                    await page.wait_for_timeout(3000)
                else:
                    await page.wait_for_timeout(2500)

            # 关闭 cookie 弹窗（先等弹窗出现，再点击）
            _cookie_closed = False
            for sel in [
                "button:has-text('Accept All')", "button:has-text('Accept all')",
                "button:has-text('Agree')", "button:has-text('OK')",
                "[data-testid='uc-accept-all-button']",   # Usercentrics
                "button[data-action='acceptAll']",
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                "[data-cmp-action='acceptAll']",
                ".osano-cm-accept-all",                    # Osano (Oliver Wyman)
                "button.osano-cm-dialog__close",
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=1500):
                        await btn.click()
                        await page.wait_for_timeout(1000)
                        _cookie_closed = True
                        print(f"         ✓ Cookie 弹窗关闭（{sel}）")
                        break
                except Exception: pass
            # Porsche Consulting 特殊处理：Usercentrics 弹窗需要额外等待
            if "porsche-consulting.com" in url and not _cookie_closed:
                await page.wait_for_timeout(2000)  # 等 Usercentrics 加载
                for sel in ["button:has-text('Accept All')", "[data-testid='uc-accept-all-button']",
                            "button:has-text('Accept')"]:
                    try:
                        btn = page.locator(sel).first
                        if await btn.is_visible(timeout=1000):
                            await btn.click()
                            await page.wait_for_timeout(1000)
                            print(f"         ✓ Usercentrics 关闭（{sel}）")
                            break
                    except Exception: pass

            # Porsche Consulting 专属：关闭所有弹窗/遮罩（包括 Filter 面板）
            if "porsche-consulting.com" in url:
                await page.evaluate("""()=>{
                    // 隐藏所有 position:fixed 的全屏遮罩
                    document.querySelectorAll('*').forEach(el=>{
                        const s = window.getComputedStyle(el);
                        const w = el.offsetWidth, h = el.offsetHeight;
                        if(s.position==='fixed' && w > 400 && h > 400){
                            el.style.setProperty('display','none','important');
                        }
                    });
                    // 隐藏所有 filter/overlay/modal 相关元素
                    document.querySelectorAll(
                        '[class*="filter"],[class*="Filter"],[class*="overlay"],'
                        +'[class*="modal"],[class*="flyout"],[class*="drawer"],'
                        +'[class*="c-filter"],[class*="c-overlay"]'
                    ).forEach(el=>{
                        el.style.setProperty('display','none','important');
                    });
                    // 点击所有可见的关闭按钮
                    document.querySelectorAll(
                        'button[aria-label="Close"],button[aria-label="close"]'
                    ).forEach(btn=>{
                        if(btn.offsetParent !== null) btn.click();
                    });
                }""")
                await page.wait_for_timeout(400)

            # 隐藏导航栏/弹窗（含 Osano cookie consent）
            # 注意：不可用 [class*="banner"] 等宽泛选择器，OW 的 <body> class 含 "banner" 会误杀整个页面
            await page.evaluate("""()=>{
                const _protect = new Set(['BODY','HTML','MAIN','ARTICLE','SECTION']);
                function _hideAll(sel){
                    try{document.querySelectorAll(sel).forEach(el=>{
                        if(_protect.has(el.tagName)) return;
                        el.style.setProperty('display','none','important');
                    })}catch(e){}
                }
                ['nav','header','footer','[role="dialog"]',
                 '.osano-cm-widget','.osano-cm-dialog','.osano-cm-window',
                 '.osano-cm-info','.osano-cm-storage','.osano-cm-save',
                 '.osano-cm-overlay','.osano-cm-drawer','[class*="osano"]',
                 '#osano-cm-dialog','#osano-cm-window',
                 '[id*="cookie"]','[id*="consent"]'
                ].forEach(_hideAll);
                // 对宽泛选择器只隐藏 position:fixed/sticky 的小元素（弹窗/浮层），保护正文
                ['[class*="cookie"]','[class*="consent"]','[class*="overlay"]',
                 '[class*="modal"]','[class*="popup"]','[class*="banner"]'
                ].forEach(sel=>{
                    try{document.querySelectorAll(sel).forEach(el=>{
                        if(_protect.has(el.tagName)) return;
                        const st=window.getComputedStyle(el);
                        const pos=st.position;
                        if(pos==='fixed'||pos==='sticky'||pos==='absolute'){
                            el.style.setProperty('display','none','important');
                        } else if(el.innerText.length < 200 && el.offsetHeight < 300){
                            el.style.setProperty('display','none','important');
                        }
                    })}catch(e){}
                });
                document.body.style.overflow='auto';
                document.documentElement.style.overflow='auto';
            }""")
            await page.wait_for_timeout(300)

            # ── 步骤0（Porsche Consulting）：滚动前二次加固可见性 ──
            if _is_porsche:
                await page.add_style_tag(content="""
                    .js-animItem, .js-anim, [class*="anim"],
                    [class*="fade"], [class*="reveal"], [class*="slide"],
                    section > div, article > div, main > div {
                        opacity: 1 !important;
                        visibility: visible !important;
                        transform: none !important;
                    }
                """)
                await page.wait_for_timeout(300)

            # ── 步骤1：先慢速滚动触发懒加载（内容加载比动画更重要）──
            # Porsche Consulting: <html> 高度被限为视口高度，window.scrollTo 无效
            # 必须先解除 <html> 高度限制才能正常滚动触发 lazy load
            if _is_porsche:
                await page.evaluate("""()=>{
                    document.documentElement.style.height = 'auto';
                    document.documentElement.style.overflow = 'auto';
                    document.body.style.overflow = 'visible';
                }""")
                await page.wait_for_timeout(200)
            await page.wait_for_timeout(500)
            _ph = await page.evaluate("document.body.scrollHeight")
            _pos = 0
            while _pos < _ph:
                _pos = min(_pos + 500, _ph)
                await page.evaluate(f"window.scrollTo(0, {_pos})")
                await page.wait_for_timeout(250)
            # 二次滚动（高度可能增加）
            _ph2 = await page.evaluate("document.body.scrollHeight")
            if _ph2 > _ph + 100:
                while _pos < _ph2:
                    _pos = min(_pos + 500, _ph2)
                    await page.evaluate(f"window.scrollTo(0, {_pos})")
                    await page.wait_for_timeout(250)
            # 等待网络请求完成（图片/内容加载）
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                await page.wait_for_timeout(2000)

            # ── 步骤2：内容加载完后，禁用动画让元素全部显示 ──────────
            await page.evaluate("""()=>{
                const s = document.createElement('style');
                s.textContent = `
                    *, *::before, *::after {
                        animation-duration: 0s !important;
                        animation-delay: 0s !important;
                        transition-duration: 0s !important;
                        transition-delay: 0s !important;
                    }
                `;
                document.head.appendChild(s);
                // 强制显示所有动画隐藏的元素（覆盖多种 class 命名规则）
                document.querySelectorAll(
                    '.js-animItem,.js-anim,[class*="is-animated"],[class*="anim-"],'
                    +'[class*="animItem"],[class*="fade-"],[class*="slide-"],'
                    +'[class*="reveal"],[data-animate],[data-aos]'
                ).forEach(el=>{
                    el.style.setProperty('opacity','1','important');
                    el.style.setProperty('visibility','visible','important');
                    el.style.setProperty('transform','none','important');
                });
            }""")
            await page.wait_for_timeout(800)

            # ── 步骤3：强制展开页面高度，再滚回顶部截图 ──────────────
            # Porsche Consulting 用 min-height:100vh，需要强制展开才能截到全页
            if "porsche-consulting.com" in url:
                await page.evaluate("""()=>{
                    // 强制所有 min-height:100vh 的容器展开为 auto
                    document.querySelectorAll('*').forEach(el=>{
                        const s = window.getComputedStyle(el);
                        if(s.minHeight && s.minHeight.includes('vh')){
                            el.style.minHeight = 'auto';
                        }
                        if(s.height && s.height.includes('vh') && el.tagName !== 'HTML' && el.tagName !== 'BODY'){
                            el.style.height = 'auto';
                        }
                    });
                }""")
                await page.wait_for_timeout(500)

            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(600)

            # Porsche Consulting：截图前最终一轮强制可见（高度展开可能触发新元素）
            if _is_porsche:
                await page.add_style_tag(content="""
                    .js-animItem, .js-anim, [class*="anim"],
                    section > div, article > div, main > div {
                        opacity: 1 !important;
                        visibility: visible !important;
                        transform: none !important;
                    }
                """)
                await page.wait_for_timeout(300)

            # 截图前确认页面高度
            _final_h = await page.evaluate("document.body.scrollHeight")
            print(f"         截图页面高度: {_final_h}px")

            await page.screenshot(path=fpath, full_page=True, type="png")
            real_html = await page.content()

            # ── Akamai / Access Denied 检测（截图可能捕获到错误页面）──
            if "Access Denied" in (real_html or "")[:3000]:
                print(f"         ⚠ 页面内容为 Access Denied，丢弃截图")
                await browser.close()
                # 尝试 Archive.org 降级
                try:
                    import urllib.parse as _up_ad
                    _ad_api = f"https://archive.org/wayback/available?url={_up_ad.quote(url, safe='')}"
                    _ad_loop = asyncio.get_running_loop()
                    def _qry_arch():
                        try:
                            r = http_get(_ad_api, timeout=10)
                            d = __import__("json").loads(r.text)
                            snap = d.get("archived_snapshots",{}).get("closest",{})
                            if snap.get("available"):
                                return snap["url"].replace("http://","https://")
                        except Exception: pass
                        return ""
                    _arch = await _ad_loop.run_in_executor(None, _qry_arch)
                    if _arch:
                        print(f"         → Archive.org 降级: {_arch[:80]}")
                        async with async_playwright() as _p2:
                            _b2 = await _p2.chromium.launch(headless=True, args=["--no-sandbox"])
                            _c2 = await _b2.new_context(viewport={"width":860,"height":1200})
                            _pg2 = await _c2.new_page()
                            try:
                                await _pg2.goto(_arch, wait_until="domcontentloaded", timeout=45000)
                                await _pg2.wait_for_timeout(2000)
                                await _pg2.evaluate("try{document.querySelector('#wm-ipp-base')?.remove();document.querySelector('#wm-ipp')?.remove();}catch(e){}")
                                await _pg2.screenshot(path=fpath, full_page=True, type="png")
                                real_html = await _pg2.content()
                                _arch_ok, _arch_reason = True, ""
                                if "mckinsey.com" in url:
                                    _arch_ok, _arch_reason = _mck_page_quality_ok(url, real_html, article)
                                await _b2.close()
                                if _arch_ok:
                                    return True, "archive.org(access_denied)", real_html
                                print(f"         ⚠ Archive.org 页面质量不足（{_arch_reason}），改走本地 HTML")
                            except Exception:
                                await _b2.close()
                except Exception: pass
                ok_lh, msg_lh = await _local_html_screenshot()
                if ok_lh:
                    return True, "fallback(access_denied)", ""
                return False, "access_denied", ""

            # ── 空白检测：若截图为空白，追加等待后重试（最多 3 次）──
            _retry_blank = 0
            while _is_blank(fpath) and _retry_blank < 3:
                _retry_blank += 1
                _extra_wait = _retry_blank * 3000   # 3s / 6s / 9s
                print(f"         ⚠ 截图为空白，追加等待 {_extra_wait//1000}s 后重试"
                      f"（第{_retry_blank}次）...")
                await page.wait_for_timeout(_extra_wait)
                # 再次移除所有弹窗/遮罩（Osano 可能在 retry 间隙重新出现）
                await page.evaluate("""()=>{
                    const _protect = new Set(['BODY','HTML','MAIN','ARTICLE','SECTION']);
                    function _hideAll(sel){
                        try{document.querySelectorAll(sel).forEach(el=>{
                            if(_protect.has(el.tagName)) return;
                            el.style.setProperty('display','none','important');
                        })}catch(e){}
                    }
                    ['[class*="osano"]','.osano-cm-dialog','.osano-cm-window',
                     '.osano-cm-widget','.osano-cm-overlay','.osano-cm-drawer',
                     '[role="dialog"]','[id*="cookie"]','[id*="consent"]',
                     'nav','header','footer'].forEach(_hideAll);
                    ['[class*="cookie"]','[class*="consent"]','[class*="overlay"]',
                     '[class*="modal"]','[class*="popup"]','[class*="banner"]'
                    ].forEach(sel=>{
                        try{document.querySelectorAll(sel).forEach(el=>{
                            if(_protect.has(el.tagName)) return;
                            const st=window.getComputedStyle(el);
                            if(st.position==='fixed'||st.position==='sticky'||st.position==='absolute'){
                                el.style.setProperty('display','none','important');
                            } else if((el.innerText||'').length < 200 && el.offsetHeight < 300){
                                el.style.setProperty('display','none','important');
                            }
                        })}catch(e){}
                    });
                    document.body.style.overflow='auto';
                    document.documentElement.style.overflow='auto';
                }""")
                # Porsche Consulting：再次强制显示所有动画元素
                if _is_porsche:
                    await page.add_style_tag(content="""
                        .js-animItem, .js-anim, [class*="anim"],
                        section > div, article > div, main > div {
                            opacity: 1 !important;
                            visibility: visible !important;
                            transform: none !important;
                        }
                    """)
                # 再次尝试等内容选择器
                for _rs in ["article", "main", "h1", "[class*='article']",
                            "[class*='content']", "[class*='insight']", "p"]:
                    try:
                        await page.wait_for_selector(_rs, timeout=5000,
                                                     state="visible")
                        break
                    except Exception:
                        pass
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(800)
                _final_h = await page.evaluate("document.body.scrollHeight")
                print(f"         重试截图，页面高度: {_final_h}px")
                await page.screenshot(path=fpath, full_page=True, type="png")
                real_html = await page.content()

            _quality_ok, _quality_reason = True, ""
            if "mckinsey.com" in url:
                _quality_ok, _quality_reason = _mck_page_quality_ok(url, real_html, article)
                if not _quality_ok:
                    print(f"         ⚠ Live 页面质量不足（{_quality_reason}），继续降级")
            await browser.close()
            real_ok = _quality_ok
    except Exception as _real_exc:
        real_ok = False
        real_html = ""
        print(f"         ⚠ 截图策略1异常: {_real_exc}")

    # 检测是否为 CF 验证页
    _is_cf_challenge = "Performing security verification" in real_html or "Just a moment" in real_html

    if real_ok and not _is_blank(fpath) and not _is_cf_challenge:
        print(f"         ✅ 截图策略1成功")
        return True, "", real_html

    # ══════════════════════════════════════════════════════
    # 策略2：CF 拦截 → 尝试 Archive.org 存档
    # ══════════════════════════════════════════════════════
    if _is_cf_challenge or not real_ok:
        try:
            import asyncio as _aio
            def _query_archive():
                try:
                    import requests as _rq
                    _hdr = {"User-Agent": "Googlebot/2.1"}
                    r = _rq.get(f"https://archive.org/wayback/available?url={url}",
                                timeout=10, headers=_hdr)
                    if r.status_code == 200:
                        snap = r.json().get("archived_snapshots", {}).get("closest", {})
                        if snap.get("status") == "200":
                            return snap["url"]
                except Exception:
                    pass
                try:
                    import requests as _rq
                    _direct = f"https://web.archive.org/web/2026/{url}"
                    _dr = _rq.head(_direct, timeout=10, allow_redirects=True,
                                   headers={"User-Agent": "Mozilla/5.0"})
                    if _dr.status_code == 200:
                        return _dr.url
                except Exception:
                    pass
                return ""
            _loop2 = asyncio.get_running_loop()
            arch_url = await _loop2.run_in_executor(None, _query_archive)
            if arch_url:
                async with async_playwright() as p:
                    b  = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                    c  = await b.new_context(viewport={"width": 1280, "height": 900})
                    pg = await c.new_page()
                    await pg.goto(arch_url, wait_until="domcontentloaded", timeout=45000)
                    await pg.wait_for_timeout(3000)
                    await pg.evaluate("""()=>{
                        ['#wm-ipp-base','#wm-ipp','#donato','#wm-ipp-print',
                         '.wb-autocomplete-suggestions','#wm-btns'].forEach(s=>{
                            const e=document.querySelector(s);
                            if(e) e.style.display='none';
                        });
                        document.body.style.marginTop='0';
                    }""")
                    arch_html = await pg.content()
                    await pg.screenshot(path=fpath, full_page=True, type="png")
                    await b.close()
                _arch_ok = not _is_blank(fpath)
                if _arch_ok and "mckinsey.com" in url:
                    _arch_ok, _arch_reason = _mck_page_quality_ok(url, arch_html, article)
                    if not _arch_ok:
                        print(f"         ⚠ Archive.org 页面质量不足（{_arch_reason}），改走本地 HTML")
                if _arch_ok:
                    print(f"         ✅ 截图策略2成功(Archive.org)")
                    return True, "archive.org", arch_html
        except Exception:
            pass

    # ══════════════════════════════════════════════════════
    # 策略3：Archive.org 也没有
    # - CF 验证页但有截图：直接使用（比本地模板真实）
    # - 截图失败：降级本地 HTML
    # ══════════════════════════════════════════════════════
    if _is_cf_challenge and real_ok and not _is_blank(fpath):
        # CF 页面截图虽不是正文，但保留原始截图，PDF 会另外处理
        print(f"         ⚠ CF拦截，截图为验证页（无Archive.org存档）")
        return True, "cf_page", real_html

    reason = "cf" if _is_cf_challenge else ("blank" if real_ok else "failed")
    print(f"         ⚠ 截图降级本地HTML(reason={reason})")
    ok, msg = await _local_html_screenshot(_shared_page)
    if ok:
        return True, f"fallback({reason})", ""
    return False, f"real={reason},fallback={msg}", ""



# ════════════════════════════════════════════════════
# 统计报告
# ════════════════════════════════════════════════════
def _check_date_vs_filename(summary, company):
    """
    核对文件名中的日期 与 PDF/截图内页实际日期 是否一致。
    文件名格式：[Company]Title-YYYY.MM.DD.pdf
    PDF 内页日期：story 里第3个 Paragraph（article["date"]）。
    用 _pdf_text_len 已有的解压逻辑提取 PDF 内文字，再用 parse_date 解析。

    返回：list[dict]  每项含 fname / filename_date / content_date / url
    """
    import zlib as _zlib, re as _re, os as _os

    mismatches = []

    def _extract_date_from_pdf(path):
        """从 PDF stream 中提取最早出现的合法日期字符串"""
        try:
            with open(path, "rb") as f: raw = f.read()
            text_buf = []
            for m in _re.finditer(rb"stream\r?\n(.*?)\r?\nendstream", raw, _re.DOTALL):
                chunk = m.group(1)
                try:    text_buf.append(_zlib.decompress(chunk).decode("latin-1", errors="ignore"))
                except Exception: text_buf.append(chunk.decode("latin-1", errors="ignore"))
            full = " ".join(text_buf)
            # 从 BT...ET 块提取文字
            chars = []
            for bt in _re.finditer(r"BT(.*?)ET", full, _re.DOTALL):
                for tj in _re.finditer(r"\((.*?)\)", bt.group(1)):
                    chars.append(tj.group(1))
            text = " ".join(chars)
            # 找 YYYY-MM-DD 或 YYYY.MM.DD
            m = _re.search(r"(20\d{2})[.\-](\d{2})[.\-](\d{2})", text)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        except Exception:
            pass
        return ""

    def _date_from_filename(fname):
        """从文件名末尾提取 YYYY.MM.DD"""
        import re as _r
        m = _r.search(r"(20\d{2})\.(\d{2})\.(\d{2})\.[a-z]+$", fname)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return ""

    for a in summary:
        url = a.get("url","")
        article_date = a.get("date","")  # summary 里记录的日期

        for ext, folder in [("pdf", PDF_DIR), ("png", SCREENSHOT_DIR)]:
            if FMT == "1" and ext == "pdf": continue
            if FMT == "2" and ext == "png": continue
            fname = a.get(ext)
            if not fname: continue
            fpath = _os.path.join(folder, fname)
            if not _os.path.exists(fpath): continue

            fn_date = _date_from_filename(fname)
            if not fn_date:
                continue  # 文件名本身没有日期，无法核对

            # PDF 可以做深度核对（读内页日期）
            content_date = ""
            if ext == "pdf":
                content_date = _extract_date_from_pdf(fpath)

            # 核对1：文件名日期 vs summary 记录日期
            if article_date and fn_date and fn_date != article_date:
                mismatches.append({
                    "fname": fname, "ext": ext,
                    "filename_date": fn_date,
                    "content_date": content_date or article_date,
                    "summary_date": article_date,
                    "url": url,
                    "issue": f"文件名日期({fn_date}) ≠ 记录日期({article_date})",
                })
                continue

            # 核对2：PDF 内页日期 vs 文件名日期
            if content_date and fn_date and content_date != fn_date:
                mismatches.append({
                    "fname": fname, "ext": ext,
                    "filename_date": fn_date,
                    "content_date": content_date,
                    "summary_date": article_date,
                    "url": url,
                    "issue": f"PDF内页日期({content_date}) ≠ 文件名日期({fn_date})",
                })

    return mismatches


def _fix_date_mismatches(mismatches, summary):
    """
    对日期不一致的文件自动重命名，让文件名日期 = content_date（以内容为准）。
    同时更新 summary 中的日期记录。
    """
    import os as _os
    fixed = 0
    for m in mismatches:
        correct_date = m.get("content_date") or m.get("summary_date")
        if not correct_date:
            continue
        fname     = m["fname"]
        ext       = m["ext"]
        folder    = PDF_DIR if ext == "pdf" else SCREENSHOT_DIR
        old_path  = _os.path.join(folder, fname)
        if not _os.path.exists(old_path):
            continue

        # 构造新文件名（替换日期部分）
        import re as _r
        new_fname = _r.sub(
            r"(20\d{2})\.(\d{2})\.(\d{2})\." + ext + r"$",
            correct_date.replace("-", ".") + "." + ext,
            fname
        )
        if new_fname == fname:
            continue

        new_path = _os.path.join(folder, new_fname)
        try:
            _os.rename(old_path, new_path)
            print(f"    🔧 重命名: {fname}")
            print(f"         → {new_fname}")
            # 同步更新 summary
            for a in summary:
                if a.get(ext) == fname:
                    a[ext] = new_fname
                    a["date"] = correct_date
            fixed += 1
        except Exception as e:
            print(f"    ⚠ 重命名失败 {fname}: {e}")
    return fixed


def _print_stats(articles, summary, company):
    """
    抓取完成后打印统计报告：
      - 时间范围内总篇数
      - 按内容类型分类（Whitepaper / Articles / Rapid Impact Analysis 等）
      - 按月份分布（柱状图）
      - 成功输出文件数
    """
    W = 62
    print(f"\n{'═'*W}")
    print(f"  📊 抓取统计报告  ·  {company}  ·  最近 {MONTHS} 个月")
    print(f"{'═'*W}")

    total = len(articles)
    done  = len(summary)
    print(f"  时间范围 : >= {CUTOFF.strftime('%Y-%m-%d')}")
    print(f"  列表页收集: {total} 篇    成功输出: {done} 篇")
    print(f"{'─'*W}")

    # ── 按内容类型统计 ───────────────────────────────
    by_ctype = {}
    for a in articles:
        ct = (a.get("_spg_ctype") or a.get("source") or "Unknown").strip() or "Unknown"
        by_ctype[ct] = by_ctype.get(ct, 0) + 1

    print(f"\n  📂 按内容类型：")
    for ct, n in sorted(by_ctype.items(), key=lambda x: -x[1]):
        bar = "▓" * min(n, 40)
        print(f"    {ct:<32s} {n:4d} 篇  {bar}")

    # ── 按月份统计 ───────────────────────────────────
    by_month = {}
    no_date = 0
    for a in articles:
        d = a.get("date") or ""
        if d and len(d) >= 7:
            mon = d[:7]
            by_month[mon] = by_month.get(mon, 0) + 1
        else:
            no_date += 1

    if by_month:
        max_n   = max(by_month.values())
        bar_max = 36
        print(f"\n  📅 按月份分布：")
        for mon in sorted(by_month.keys(), reverse=True):
            n = by_month[mon]
            bar_len = max(1, round(n / max_n * bar_max)) if max_n else 0
            bar = "█" * bar_len
            print(f"    {mon}   {n:4d} 篇  {bar}")
        if no_date:
            print(f"    (日期未知)  {no_date:4d} 篇")

    # ── 输出文件质量小结 ─────────────────────────────
    if summary:
        has_pdf = sum(1 for a in summary if a.get("pdf"))
        has_png = sum(1 for a in summary if a.get("png"))
        print(f"\n  💾 输出文件：")
        if FMT in ("2", "3"):
            print(f"    PDF  : {has_pdf:4d} 个")
        if FMT in ("1", "3"):
            print(f"    截图 : {has_png:4d} 个")

    print(f"{'═'*W}\n")

# ════════════════════════════════════════════════════
# 自检：验证输出文件质量
# ════════════════════════════════════════════════════
def verify_outputs(summary, company):
    """
    两级检测：
      Level-1  文件大小（原有逻辑）
      Level-2  内容检测：PNG 像素采样判空白；PDF 文字提取判无正文
    返回：(all_ok: bool, blank_items: list[dict])
    """
    import struct, zlib as _zlib, re as _re2

    def _png_dims(path):
        try:
            with open(path, "rb") as f:
                sig = f.read(8)
                if sig != b"\x89PNG\r\n\x1a\n": return None
                f.seek(16)
                w, h = struct.unpack(">II", f.read(8))
                return w, h
        except Exception: return None

    def _is_png_blank(path, blank_ratio=0.97):
        """采样像素，97% 以上近白色 → 空白页"""
        try:
            with open(path, "rb") as f: data = f.read()
            if data[:8] != b"\x89PNG\r\n\x1a\n": return False
            w  = struct.unpack(">I", data[16:20])[0]
            h  = struct.unpack(">I", data[20:24])[0]
            bd = data[24]; ct = data[25]
            if bd != 8 or ct not in (2, 6): return False
            idat = bytearray()
            pos = 8
            while pos < len(data) - 12:
                length = struct.unpack(">I", data[pos:pos+4])[0]
                ctype  = data[pos+4:pos+8]
                if ctype == b"IDAT": idat += data[pos+8:pos+8+length]
                elif ctype == b"IEND": break
                pos += 12 + length
            raw  = _zlib.decompress(bytes(idat))
            bpp  = 4 if ct == 6 else 3
            stride = 1 + w * bpp
            white = total = 0
            rstep = max(1, h // 40); cstep = max(1, w // 50)
            for ri in range(0, h, rstep):
                rs = ri * stride + 1
                for ci in range(0, w, cstep):
                    ps = rs + ci * bpp
                    if ps + bpp > len(raw): continue
                    r, g, b = raw[ps], raw[ps+1], raw[ps+2]
                    total += 1
                    if r >= 230 and g >= 230 and b >= 230: white += 1
            return (white / total) >= blank_ratio if total else False
        except Exception: return False

    def _a85decode(data):
        text = data.decode('ascii', errors='ignore')
        text = ''.join(text.split())
        if text.endswith('~>'): text = text[:-2]
        result = bytearray()
        i = 0
        while i < len(text):
            if text[i] == 'z':
                result.extend(b'\x00\x00\x00\x00'); i += 1
            else:
                grp = text[i:i+5]; i += 5
                padded = grp.ljust(5, 'u')
                val = 0
                for c in padded: val = val * 85 + (ord(c) - 33)
                bs = val.to_bytes(4, 'big')
                result.extend(bs[:len(grp)-1] if len(grp) < 5 else bs)
        return bytes(result)

    def _pdf_text_len(path):
        """解压 PDF stream（支持 ASCII85Decode+FlateDecode），统计 BT...ET 内文字字符数"""
        try:
            with open(path, "rb") as f: raw = f.read()
            text_buf = []
            for m in _re2.finditer(rb"stream\s*\n(.*?)endstream", raw, _re2.DOTALL):
                chunk = m.group(1).rstrip()
                obj_start = raw.rfind(b"obj", 0, m.start())
                filter_region = raw[max(0, obj_start):m.start()]
                is_a85 = b"ASCII85Decode" in filter_region
                try:
                    if is_a85:
                        chunk = _a85decode(chunk)
                    text_buf.append(_zlib.decompress(chunk).decode("latin-1", errors="ignore"))
                except Exception:
                    text_buf.append(chunk.decode("latin-1", errors="ignore"))
            full = " ".join(text_buf)
            chars = 0
            for bt in _re2.finditer(r"BT(.*?)ET", full, _re2.DOTALL):
                for tj in _re2.finditer(r"\(([^)]*)\)", bt.group(1)):
                    chars += len(tj.group(1).strip())
                for tj in _re2.finditer(r"<([0-9a-fA-F]{4,})>", bt.group(1)):
                    chars += len(tj.group(1)) // 4
            return chars
        except Exception: return -1

    def _pdf_has_embedded_image(path):
        try:
            with open(path, "rb") as f:
                raw = f.read()
            return (b"/Subtype /Image" in raw) or (b"/Image" in raw and b"/XObject" in raw)
        except Exception:
            return False

    print(f"\n{'='*60}")
    print("🔍 自检结果（内容级检测）：")
    ok_count = 0
    issues = []
    blank_items = []

    for a in summary:
        item_blank = False
        expected = {"png": a.get("png"), "pdf": a.get("pdf")}
        base = make_filename(company, a.get("title",""), a.get("date",""), "")

        for ext, min_kb, label, folder in [
            ("png", 25, "截图", SCREENSHOT_DIR),
            ("pdf",  5, "PDF",  PDF_DIR),
        ]:
            if FMT == "1" and ext == "pdf": continue
            if FMT == "2" and ext == "png": continue
            fname = expected.get(ext) or (base + ext)
            p = os.path.join(folder, fname)

            if not os.path.exists(p):
                issues.append(f"⚠ {label}文件缺失: {fname}")
                item_blank = True
                continue

            kb = os.path.getsize(p) // 1024
            if kb < min_kb:
                issues.append(f"⚠ {label}过小({kb}KB): {fname}")
                item_blank = True
                continue

            if ext == "png":
                dims = _png_dims(p)
                if dims and dims[0] < 820:
                    issues.append(f"⚠ 截图宽度异常({dims[0]}px): {fname}")
                    item_blank = True
                    continue
                if _is_png_blank(p):
                    issues.append(f"⚠ 截图为空白页({kb}KB): {fname}")
                    item_blank = True
                    continue
                ok_count += 1
            elif ext == "pdf":
                tlen = _pdf_text_len(p)
                if 0 <= tlen < 80 and not (kb >= 40 and _pdf_has_embedded_image(p)):
                    issues.append(f"⚠ PDF正文为空({kb}KB, 文字={tlen}字符): {fname}")
                    item_blank = True
                    continue
                ok_count += 1

        if item_blank:
            blank_items.append(a)

    if issues:
        print(f"  发现 {len(issues)} 个问题：")
        for i in issues: print(f"  {i}")
    else:
        print(f"  ✅ 全部 {ok_count} 个文件均正常（含内容级检测）")
    print(f"{'='*60}")
    # ── 日期核对 ──────────────────────────────────────
    print("\n  📅 日期核对（文件名 vs 内容日期）：")
    mismatches = _check_date_vs_filename(summary, company)
    if mismatches:
        print(f"  ⚠ 发现 {len(mismatches)} 处日期不一致，自动修正...")
        fixed = _fix_date_mismatches(mismatches, summary)
        print(f"  ✅ 已修正 {fixed} 个文件名")
        for mm in mismatches:
            print(f"    - {mm['issue']}")
            print(f"      {mm['fname']}")
    else:
        print("  ✅ 所有文件名日期与内容日期一致")

    return len(issues) == 0, blank_items


def export_summary_excel(outbase, summary_items, meta):
    """导出 Excel 汇总：第一页就是完整链接清单，打开即可见。"""
    if not OPENPYXL_AVAILABLE:
        print("  ⚠ openpyxl 不可用，跳过 Excel 汇总导出")
        return None

    xlsx_path = os.path.join(outbase, "news_links_summary.xlsx")
    wb = Workbook()
    ws = wb.active
    ws.title = "News Links"

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = XLFont(color="FFFFFF", bold=True, size=11)
    bold_font = XLFont(bold=True)
    link_font = XLFont(color="0563C1", underline="single", size=10)
    normal_font = XLFont(size=10)
    title_font = XLFont(bold=True, size=14, color="1F4E78")
    meta_font = XLFont(size=10, color="555555")

    def _style_header_row(sheet, row_idx):
        for cell in sheet[row_idx]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")

    company = meta.get("company") or ""
    generated = meta.get("generated") or datetime.now().isoformat(timespec="seconds")
    total = len(summary_items)

    ws.merge_cells("A1:D1")
    title_cell = ws.cell(row=1, column=1, value=f"{company} News Links Summary")
    title_cell.font = title_font
    title_cell.alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 32

    info_lines = [
        f"Source: {meta.get('url', '')}",
        f"Generated: {generated[:19]}   |   Total: {total}   |   Success: {meta.get('success', total)}   |   Failed: {meta.get('failed', 0)}",
    ]
    for i, line in enumerate(info_lines):
        ws.merge_cells(start_row=2 + i, start_column=1, end_row=2 + i, end_column=4)
        c = ws.cell(row=2 + i, column=1, value=line)
        c.font = meta_font
        c.alignment = Alignment(vertical="center")

    header_row = 2 + len(info_lines) + 1
    headers = ["#", "Date", "Title", "URL"]
    for col_idx, h in enumerate(headers, 1):
        ws.cell(row=header_row, column=col_idx, value=h)
    _style_header_row(ws, header_row)
    ws.freeze_panes = f"A{header_row + 1}"
    ws.auto_filter.ref = f"A{header_row}:D{header_row}"

    for item in summary_items:
        r = ws.max_row + 1
        ws.cell(row=r, column=1, value=item.get("index", "")).font = normal_font
        ws.cell(row=r, column=2, value=item.get("date", "")).font = normal_font
        ws.cell(row=r, column=3, value=item.get("title", "")).font = normal_font
        url = item.get("url") or ""
        url_cell = ws.cell(row=r, column=4, value=url)
        if url:
            url_cell.hyperlink = url
            url_cell.font = link_font
        else:
            url_cell.font = normal_font
        for col in range(1, 5):
            ws.cell(row=r, column=col).alignment = Alignment(vertical="top", wrap_text=(col >= 3))

    ws.column_dimensions["A"].width = 6
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 58
    ws.column_dimensions["D"].width = 96

    total_row = ws.max_row + 2
    ws.cell(row=total_row, column=1, value="Total").font = bold_font
    ws.cell(row=total_row, column=2, value=total).font = bold_font

    ws2 = wb.create_sheet("Details")
    detail_headers = ["#", "Date", "Title", "URL", "PDF File", "PNG File"]
    ws2.append(detail_headers)
    _style_header_row(ws2, 1)
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = "A1:F1"

    for item in summary_items:
        r = ws2.max_row + 1
        ws2.append([
            item.get("index", ""),
            item.get("date", ""),
            item.get("title", ""),
            item.get("url", ""),
            item.get("pdf", ""),
            item.get("png", ""),
        ])
        url = item.get("url") or ""
        if url:
            cell = ws2.cell(row=r, column=4)
            cell.hyperlink = url
            cell.font = link_font

    for col, w in {"A": 6, "B": 14, "C": 54, "D": 96, "E": 48, "F": 48}.items():
        ws2.column_dimensions[col].width = w
    for row in ws2.iter_rows(min_row=2, min_col=3, max_col=6):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    wb.save(xlsx_path)
    return xlsx_path


# ════════════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════════════
async def main():
    font    = reg_font()
    company = get_company_name(original_url)

    # ════════════════════════════════════════════════════
    # 单篇文章模式：直接处理 LIST_URL 这一篇，跳过列表抓取
    # ════════════════════════════════════════════════════
    if SINGLE_ARTICLE_MODE:
        url = LIST_URL.strip()
        print(f"\n{'='*60}")
        print(f"  📄 单篇模式")
        print(f"  URL   : {url}")
        print(f"  格式  : {'截图' if FMT=='1' else 'PDF' if FMT=='2' else '截图+PDF'}")
        print(f"  输出  : {OUTBASE}")
        print(f"  引擎  : curl_cffi={'✓' if CURL_AVAILABLE else '✗'}  "
              f"patchright={'✓' if PATCHRIGHT_AVAILABLE else '✗'}")
        print(f"{'='*60}\n")

        # 抓取正文
        pg_title, body, pg_date = fetch_article_body(url, article=article)
        title = pg_title if (pg_title and len(pg_title) > 5) else url.rstrip("/").split("/")[-1].replace("-", " ").title()
        date  = pg_date or datetime.now().strftime("%Y-%m-%d")

        if body:
            print(f"  ✓ 正文抓取成功，{len(body)} 段")
        else:
            _preview_body = _best_effort_preview_blocks({
                "url": url, "title": title, "summary": "",
            })
            if _preview_body:
                print(f"  ⚠ 正文为空，改用预览正文 {len(_preview_body)} 段")
                body = _preview_body
            else:
                print("  ⚠ 正文为空，将仅保存标题和链接")
                body = [{"tag": "p", "text": f"请访问原文: {url}"}]

        article = {"title": title, "date": date, "url": url, "summary": "", "source": "single"}
        body = _normalize_mck_theme_body(article, body)
        pdf_name = png_name = None

        # 生成截图
        if FMT in ("1", "3"):
            fname = make_filename(company, title, date, "png")
            fpath = os.path.join(SCREENSHOT_DIR, fname)
            ok, err, _ = await take_screenshot(url, fpath, article=article, body=body)
            if ok:
                kb = os.path.getsize(fpath) // 1024
                print(f"  📸 {fname} ({kb}KB)")
                png_name = fname
            else:
                print(f"  ⚠ 截图失败: {err}")

        # 生成 PDF（在截图之后，便于薄正文场景直接嵌图）
        if FMT in ("2", "3"):
            fname = make_filename(company, title, date, "pdf")
            fpath = os.path.join(PDF_DIR, fname)
            _png_for_pdf_single = os.path.join(SCREENSHOT_DIR, png_name) if png_name else None
            build_pdf(article, body, font, fpath, _png_for_pdf_single)
            kb = os.path.getsize(fpath) // 1024
            print(f"  📄 {fname} ({kb}KB)")
            pdf_name = fname

        # 保存 summary.json
        summary_data = [{
            "index": 1, "date": date, "title": title,
            "url": url, "pdf": pdf_name, "png": png_name,
        }]
        with open(os.path.join(OUTBASE, "summary.json"), "w", encoding="utf-8") as f:
            json.dump({
                "mode": "single_article", "url": url,
                "generated": datetime.now().isoformat(),
                "articles": summary_data,
            }, f, ensure_ascii=False, indent=2)
        xlsx_path = export_summary_excel(OUTBASE, summary_data, {
            "company": company,
            "mode": "single_article",
            "url": url,
            "generated": datetime.now().isoformat(),
            "total": 1,
            "success": len(summary_data),
            "failed": 0,
            "skipped": 0,
        })

        print(f"\n✅ 完成！输出: {OUTBASE}")
        if FMT in ("1","3"): print(f"   截图: {SCREENSHOT_DIR}")
        if FMT in ("2","3"): print(f"   PDF:  {PDF_DIR}")
        if xlsx_path: print(f"   Excel: {xlsx_path}")
        return   # ← 单篇模式到此结束，不走后续列表流程

    print(f"\n{'='*60}")
    print(f"公司: [{company}]  字体: {font}")
    print(f"输出: {OUTBASE}")
    if _CUTOFF_OVERRIDE is not None:
        print(f"范围: {CUTOFF.strftime('%Y-%m-%d')} 至今（精确日期）")
    else:
        print(f"范围: 最近 {MONTHS} 个月 (>= {CUTOFF.strftime('%Y-%m-%d')})")
    print(f"格式: {'截图' if FMT=='1' else 'PDF' if FMT=='2' else '截图+PDF'}")
    print(f"引擎: curl_cffi={'✓' if CURL_AVAILABLE else '✗'}  "
          f"patchright={'✓' if PATCHRIGHT_AVAILABLE else '✗'}  "
          f"browserforge={'✓' if BROWSERFORGE_AVAILABLE else '✗'}")
    print(f"{'='*60}")

    # ── 第一步：获取文章列表 ──────────────────────────
    print("\n【第一步】获取文章列表...")

    IS_SCH = "schaeffler.com" in LIST_URL
    IS_RB_MND = "mynewsdesk.com" in (LIST_URL.lower() + " " + original_url.lower())
    IS_RB_SITE = "rolandberger.com" in (LIST_URL.lower() + " " + original_url.lower()) and not IS_RB_MND

    IS_PC_NEWS_TRENDS = "porsche-consulting.com" in original_url.lower() and "/insights/news-trends" in original_url.lower()
    IS_PC  = (not IS_PC_NEWS_TRENDS) and ("porsche-consulting.com" in original_url.lower() or               "newsroom.porsche.com/en/company/porsche-consulting" in LIST_URL.lower())
    IS_SPG = "spglobal.com" in original_url.lower() and "/automotive-insights/" in original_url.lower()
    IS_WM  = "woodmac.com" in original_url.lower()
    IS_MS  = "morganstanley.com" in original_url.lower()
    IS_MCK = "mckinsey.com" in original_url.lower()
    IS_KEA = "kearney.com" in original_url.lower()
    IS_OW  = "oliverwyman.com" in original_url.lower()
    IS_BAIN = "bain.com" in original_url.lower()
    IS_BCG = "bcg.com" in original_url.lower()

    if IS_SCH:
        print("  → Schaeffler 专属模式")
        articles = fetch_schaeffler(MONTHS, CUTOFF)
    elif IS_BAIN:
        print("  → Bain & Company 专属模式（Wayback + 搜索引擎，CF 无法直接访问）")
        articles = fetch_bain_insights(LIST_URL, CUTOFF)
    elif IS_BCG:
        print("  → BCG 专属模式（Fragment POST API 分页）")
        articles = fetch_bcg(LIST_URL, CUTOFF)
    elif IS_OW:
        print("  → Oliver Wyman 专属模式（AEM 聚合 .ow.json）")
        articles = fetch_oliverwyman_insights(LIST_URL, CUTOFF)
    elif IS_SPG:
        print("  → S&P Global Mobility（Automotive Insights）专属模式")
        articles = fetch_spglobal(original_url, MONTHS, CUTOFF)
    elif IS_WM:
        print("  → Wood Mackenzie 专属模式（内部 API）")
        articles = fetch_woodmac(original_url, MONTHS, CUTOFF)
    elif IS_MS:
        print("  → Morgan Stanley 专属模式（curl_cffi）")
        articles = fetch_morganstanley(original_url, MONTHS, CUTOFF)
    elif IS_MCK:
        print("  → McKinsey 专属模式（ScrapingBee stealth_proxy）")
        articles = fetch_mckinsey(original_url, MONTHS, CUTOFF)
    elif IS_KEA:
        print("  → Kearney 专属模式（ScrapingBee stealth_proxy）")
        articles = fetch_kearney(original_url, MONTHS, CUTOFF)
    elif IS_RB_MND:
        print("  → Roland Berger（mynewsdesk）专属模式")
        articles = fetch_rolandberger(MONTHS, CUTOFF)
    elif IS_RB_SITE:
        print("  → Roland Berger（rolandberger.com）专属模式")
        articles = await fetch_rolandberger_site(original_url, CUTOFF)
    elif IS_PC_NEWS_TRENDS:
        print("  → Porsche Consulting（news-trends）专属模式")
        articles = fetch_porsche_news_trends(original_url, CUTOFF)
    elif IS_PC:
        print("  → Porsche Consulting 专属模式")
        articles = fetch_porsche_consulting(MONTHS, CUTOFF)
    else:
        print("  → 通用 Playwright 模式")
        articles = await fetch_generic(LIST_URL, CUTOFF)

    total = len(articles)
    print(f"\n✅ 共收集 {total} 篇文章")
    if not total:
        print("❌ 未找到文章，请检查网址")
        return

    # ── 抽样验真：Roland Berger 站内模式日期来源 ─────────
    if IS_RB_SITE:
        try:
            sample_n = min(5, total)
            sample = random.sample(articles, k=sample_n) if total >= sample_n else list(articles)
            print(f"\n【抽样验真】随机抽查 {len(sample)} 篇的发布日期来源：")
            for j, a in enumerate(sample, 1):
                u = a.get("url", "")
                t = (a.get("title", "") or "")[:70]
                if not u:
                    continue
                print(f"  ({j}/{len(sample)}) {t}")
                _ = fetch_article_body(u, debug=True)
        except Exception as e:
            print(f"  ⚠ 抽样验真失败: {e}")

    # ── 第二步：并发处理文章（复用单个 Chromium 实例）──────
    if _RICH_AVAILABLE:
        _console.print(f"\n[bold]【第二步】处理 {total} 篇文章（并发加速）...[/bold]\n")
    else:
        print(f"\n【第二步】处理 {total} 篇文章（并发加速）...\n")

    success = failed = skipped = 0
    summary = []
    used_filenames = set()
    # 线程安全的锁，保护 used_filenames / summary / counters
    _lock = asyncio.Lock()

    # ── 启动共享 Chromium（截图复用，避免每篇都启动/关闭） ──
    _pw_ctx_mgr = async_playwright()
    _pw         = await _pw_ctx_mgr.__aenter__()
    _shared_browser = await _pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--force-color-profile=srgb", "--disable-lcd-text",
              "--disable-dev-shm-usage", "--disable-gpu"]
    )
    _shared_bctx = await _shared_browser.new_context(
        viewport={"width": 860, "height": 1200}, color_scheme="light"
    )

    # McKinsey: Akamai 反爬非常激进，必须单线程 + 请求间隔
    _is_mckinsey = "mckinsey.com" in LIST_URL
    _is_headed_site = any(d in LIST_URL for d in ["mckinsey.com", "kearney.com",
                                                    "accenture.com", "deloitte.com"])
    if _is_mckinsey:
        CONCURRENCY = 1
    elif FMT in ("1", "3"):
        CONCURRENCY = 2 if _is_headed_site else 3
    else:
        CONCURRENCY = 4 if _is_headed_site else 6

    # semaphore 控制同时运行的 worker 数
    _sem = asyncio.Semaphore(CONCURRENCY)

    async def _process_one(i, article):
        """处理单篇文章：抓正文 → 生成 PDF → 截图"""
        nonlocal success, failed, skipped
        title = article.get("title", "")
        date  = article.get("date", "")
        url   = article.get("url", "")

        async with _sem:
            pdf_name = png_name = None
            try:
                # fetch_article_body 是同步函数，用 executor 跑避免阻塞 event loop
                _ev_loop = asyncio.get_running_loop()
                pg_title, body, pg_date = await _ev_loop.run_in_executor(
                    None, fetch_article_body, url, False, article
                )
                if pg_title and len(pg_title) > 10:
                    article["title"] = pg_title
                    title = pg_title
                if pg_date:
                    article["date"] = pg_date
                    date = pg_date

                if date:
                    ddt = parse_date(date)
                    if ddt and ddt < CUTOFF:
                        async with _lock:
                            skipped += 1
                        print(f"  [{i:02d}/{total}] ⏭️ 超出范围: {title[:45]}...")
                        return
                    if ddt and ddt > datetime.now() + timedelta(days=1):
                        article["date"] = ""
                        date = ""

                # 兜底正文
                if not body:
                    body = _best_effort_preview_blocks(article)

                    fb          = article.get("_fallback_body_txt") or []
                    spg_excerpt = article.get("_spg_excerpt") or ""
                    spg_desc    = article.get("_spg_description") or ""
                    spg_ctype   = article.get("_spg_ctype") or ""
                    spg_themes  = article.get("_spg_themes") or []
                    if not body and fb:
                        body = [{"tag": "p", "text": t} for t in fb if isinstance(t, str) and t.strip()]
                    elif not body and spg_excerpt:
                        paras = [p.strip() for p in spg_excerpt.split("\n") if len(p.strip()) > 30]
                        body  = [{"tag": "p", "text": p} for p in paras] if paras \
                                else [{"tag": "p", "text": spg_excerpt}]
                    elif not body and (spg_desc or article.get("summary")):
                        body = [{"tag": "p", "text": spg_desc or article.get("summary", "")}]
                    elif not body:
                        body = []
                body = _normalize_mck_theme_body(article, body)
                if article.get("source") == "spglobal":
                    meta_parts = []
                    if spg_ctype:  meta_parts.append(f"Content Type: {spg_ctype}")
                    if spg_themes: meta_parts.append(f"Themes: {', '.join(spg_themes)}")
                    if meta_parts:
                        body.append({"tag": "h3", "text": "Article Information"})
                        for mp in meta_parts: body.append({"tag": "p", "text": mp})
                    if not body:
                        body.append({"tag": "p", "text":
                            "Full article content requires S&P Global access. "
                            "Please visit the original URL."})
                    body.append({"tag": "p", "text": f"Original article: {url}"})

                body_len = len(body)
                rendered_html = ""   # 截图时拿到的渲染 HTML，供 PDF 复用

                # ── 截图先跑（策略1会返回渲染 HTML，供 PDF 正文提取复用）──
                png_kb = 0
                if FMT in ("1", "3"):
                    async with _lock:
                        fname = make_filename(company, title, date, "png")
                        if fname in used_filenames:
                            base = fname[:-4] if fname.lower().endswith(".png") else fname
                            n = 2
                            while f"{base}-{n}.png" in used_filenames: n += 1
                            fname = f"{base}-{n}.png"
                        used_filenames.add(fname)
                        png_name = fname
                    fpath_png = os.path.join(SCREENSHOT_DIR, fname)
                    ok, err, rendered_html = await take_screenshot(
                            url, fpath_png, article=article, body=body)
                    png_kb = os.path.getsize(fpath_png) // 1024 if ok else 0

                # ── 若截图拿到了真实 HTML，用它补充/替换正文 ──────────
                # 只有真实页面渲染的HTML才用于补充正文，Archive/降级的HTML跳过
                _rendered_is_real = rendered_html and not any(
                    x in (err or "").lower()
                    for x in ["archive", "akamai", "fallback", "local_html", "cf_local"]
                )
                if _rendered_is_real and (not body or len(body) < 3 or _body_looks_teaser(article, body)):
                    try:
                        from bs4 import BeautifulSoup as _BS4
                        _soup = _BS4(rendered_html, "html.parser")
                        for _t in _soup(["nav","header","footer","script","style","noscript"]):
                            _t.decompose()
                        _skip = ["cookie","privacy","newsletter","sign up",
                                 "register","log in","share this","related articles"]
                        _body2 = []
                        # 尝试多个正文容器 selector
                        _container = None
                        for _sel in [
                            ".mdc-o-content-body", "[data-test-id='article-body']",
                            "article", "[role='main']",
                            "[class*='article']", "[class*='insight']",
                            "[class*='content']", "[class*='rich-text']",
                            "[class*='prose']", "main",
                        ]:
                            _el = _soup.select_one(_sel)
                            if _el and len(_el.get_text(strip=True)) > 200:
                                _container = _el
                                break
                        _src = _container if _container else _soup
                        for _el in _src.find_all(["p","h2","h3","h4","li"]):
                            _txt = _el.get_text(strip=True)
                            if not _txt or len(_txt) < 20: continue
                            if any(s in _txt.lower() for s in _skip): continue
                            _body2.append({"tag": _el.name, "text": _txt})
                            if len(_body2) >= 80: break
                        if len(_body2) > len(body) and not _body_looks_teaser(article, _body2):
                            body = _body2
                            body_len = len(body)
                            print(f"    ↑ 从截图 HTML 补充正文 {body_len} 段")
                    except Exception as _e:
                        pass

                # ── 生成 PDF（使用最终 body，可能已由截图 HTML 补充）────
                if FMT in ("2", "3"):
                    async with _lock:
                        fname = make_filename(company, title, date, "pdf")
                        if fname in used_filenames:
                            base = fname[:-4] if fname.lower().endswith(".pdf") else fname
                            n = 2
                            while f"{base}-{n}.pdf" in used_filenames: n += 1
                            fname = f"{base}-{n}.pdf"
                        used_filenames.add(fname)
                        pdf_name = fname
                    fpath_pdf = os.path.join(PDF_DIR, fname)
                    # 截图路径传给 build_pdf，图表类页面直接嵌图
                    # CF 验证页截图不嵌入 PDF
                    _png_for_pdf = None
                    if FMT in ("1","3") and png_name and ok:
                        _err_lower = (err or "").lower()
                        # cf验证页/akamai降级/archive降级的截图都不嵌入PDF，只有真实页面截图才嵌
                        _is_degraded_png = (
                            _err_lower.startswith("cf") or
                            "akamai" in _err_lower or
                            "archive" in _err_lower or
                            "fallback" in _err_lower or
                            "local_html" in _err_lower
                        )
                        if not _is_degraded_png:
                            _png_for_pdf = fpath_png
                    await _ev_loop.run_in_executor(
                        None, build_pdf, article, body, font, fpath_pdf, _png_for_pdf
                    )
                    kb = os.path.getsize(fpath_pdf) // 1024

                # ── 汇总 ────────────────────────────────────────
                async with _lock:
                    success += 1
                    _summary_item = {
                        "index": i, "date": date,
                        "title": article.get("title", ""), "url": url,
                        "pdf": pdf_name, "png": png_name,
                    }
                    for _sk in ("summary", "_api_body_html", "_fallback_body_txt",
                                "_spg_excerpt", "_spg_description", "_spg_ctype",
                                "_spg_themes", "source"):
                        if article.get(_sk):
                            _summary_item[_sk] = article[_sk]
                    summary.append(_summary_item)

                # 打印进度（合并一行）
                parts = [f"[{i:02d}/{total}] {date or '?'} | {title[:40]}..."]
                if body_len: parts.append(f"正文{body_len}段")
                if pg_date:  parts.append(f"📅{pg_date}")
                if FMT in ("2","3") and pdf_name:
                    parts.append(f"📄{kb}KB")
                if FMT in ("1","3") and png_name:
                    stat = "✅" if png_kb >= 50 else "⚠"
                    parts.append(f"📸{png_kb}KB{stat}")
                print("  " + "  ".join(parts))

            except Exception as e:
                async with _lock:
                    failed += 1
                print(f"  [{i:02d}/{total}] ❌ {title[:40]}... → {e}")

            # McKinsey: 请求间隔防止触发 Akamai 封锁
            if _is_mckinsey:
                _delay = random.uniform(3.0, 6.0)
                await asyncio.sleep(_delay)

    # 建立任务并发执行
    tasks = [_process_one(i, article) for i, article in enumerate(articles, 1)]
    await asyncio.gather(*tasks)

    # 关闭共享 Chromium
    await _shared_browser.close()
    await _pw_ctx_mgr.__aexit__(None, None, None)


    # ── 保存汇总 ──────────────────────────────────────
    with open(os.path.join(OUTBASE, "summary.json"), "w", encoding="utf-8") as f:
        json.dump({
            "url": LIST_URL, "months": MONTHS, "total": total,
            "success": success, "failed": failed, "skipped": skipped,
            "engines": {
                "curl_cffi": CURL_AVAILABLE,
                "patchright": PATCHRIGHT_AVAILABLE,
                "browserforge": BROWSERFORGE_AVAILABLE,
            },
            "generated": datetime.now().isoformat(),
            "articles": summary
        }, f, ensure_ascii=False, indent=2)

    # ── 自检 + 空白文件自动重新生成 ──────────────────
    all_ok, blank_items = verify_outputs(summary, company)

    if blank_items:
        if os.environ.get("NEWS_SKIP_BLANK_REGEN", "").lower() in ("1", "true", "yes"):
            print("\n⚠ NEWS_SKIP_BLANK_REGEN=1，跳过空白文件二次重生成（避免长时间卡在 Archive）")
            blank_items = []
    if blank_items:
        _regen_cap = int(os.environ.get("NEWS_BLANK_REGEN_MAX", "40") or "40")
        if _regen_cap > 0 and len(blank_items) > _regen_cap:
            print(f"\n⚠ 空白文件过多（{len(blank_items)}），仅重生成前 {_regen_cap} 篇（可调 NEWS_BLANK_REGEN_MAX）")
            blank_items = blank_items[:_regen_cap]
        print(f"\n🔁 发现 {len(blank_items)} 篇空白文件，自动重新生成...")
        regen_ok = regen_fail = 0
        for a in blank_items:
            url_r   = a.get("url","")
            title_r = a.get("title","")
            date_r  = a.get("date","")
            print(f"  重生成: {date_r or '?'} | {title_r[:50]}...")
            try:
                # 重新抓取正文
                pg_title, body_r, pg_date = fetch_article_body(url_r, article=a)
                if pg_title and len(pg_title) > 10: title_r = pg_title
                if pg_date: date_r = pg_date

                # 同样的兜底逻辑
                if not body_r:
                    body_r = _best_effort_preview_blocks(a)
                    fb = a.get("_fallback_body_txt") or []
                    spg_excerpt = a.get("_spg_excerpt") or ""
                    spg_desc    = a.get("_spg_description") or ""
                    spg_ctype   = a.get("_spg_ctype") or ""
                    spg_themes  = a.get("_spg_themes") or []
                    if not body_r and fb:
                        body_r = [{"tag":"p","text":t} for t in fb if isinstance(t,str) and t.strip()]
                    elif not body_r and spg_excerpt:
                        paras  = [p.strip() for p in spg_excerpt.split("\n") if len(p.strip())>30]
                        body_r = [{"tag":"p","text":p} for p in paras] if paras else [{"tag":"p","text":spg_excerpt}]
                    elif not body_r and (spg_desc or a.get("summary")):
                        body_r = [{"tag":"p","text": spg_desc or a.get("summary","")}]
                    elif not body_r:
                        body_r = []
                    body_r = _normalize_mck_theme_body(a, body_r)
                    if a.get("source") == "spglobal":
                        meta_parts = []
                        if spg_ctype:  meta_parts.append(f"Content Type: {spg_ctype}")
                        if spg_themes: meta_parts.append(f"Themes: {', '.join(spg_themes)}")
                        if meta_parts:
                            body_r.append({"tag":"h3","text":"Article Information"})
                            for mp in meta_parts: body_r.append({"tag":"p","text":mp})
                        if not body_r:
                            body_r.append({"tag":"p","text":
                                "Full article content requires S&P Global access. "
                                "Please visit the original URL."})
                        body_r.append({"tag":"p","text":f"Original article: {url_r}"})

                art_r = dict(a, title=title_r, date=date_r)
                _regen_png_path = os.path.join(SCREENSHOT_DIR, a["png"]) if a.get("png") else None

                if FMT in ("1","3") and a.get("png"):
                    fpath_png = os.path.join(SCREENSHOT_DIR, a["png"])
                    _regen_shot_timeout = float(os.environ.get("NEWS_BLANK_REGEN_SHOT_TIMEOUT", "240") or "240")
                    try:
                        ok_r, err_r, _ = await asyncio.wait_for(
                            take_screenshot(url_r, fpath_png, article=art_r, body=body_r),
                            timeout=_regen_shot_timeout)
                    except asyncio.TimeoutError:
                        ok_r, err_r = False, f"截图重生成超时（>{int(_regen_shot_timeout)}s）"
                    if ok_r:
                        kb = os.path.getsize(fpath_png) // 1024
                        print(f"    📸 重生成截图 {kb}KB")
                        _regen_png_path = fpath_png
                    else:
                        print(f"    ⚠ 截图重生成失败: {err_r}")

                if FMT in ("2","3") and a.get("pdf"):
                    fpath_pdf = os.path.join(PDF_DIR, a["pdf"])
                    build_pdf(art_r, body_r, font, fpath_pdf, _regen_png_path)
                    kb = os.path.getsize(fpath_pdf) // 1024
                    print(f"    📄 重生成 PDF {kb}KB")

                regen_ok += 1
            except Exception as e:
                print(f"    ❌ 重生成失败: {e}")
                regen_fail += 1

        print(f"  重生成完成: 成功 {regen_ok} 篇  失败 {regen_fail} 篇")

        # 二次自检
        all_ok, _ = verify_outputs(summary, company)

    # ── 统计报告 ───────────────────────────────────────
    _print_stats(articles, summary, company)
    xlsx_path = export_summary_excel(OUTBASE, summary, {
        "company": company,
        "mode": "batch",
        "url": LIST_URL,
        "generated": datetime.now().isoformat(),
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
    })

    print(f"\n✅ 全部完成！")
    print(f"   成功: {success} 篇  失败: {failed} 篇")
    if skipped:
        print(f"   跳过: {skipped} 篇（超出日期范围）")
    print(f"   输出: {OUTBASE}")
    if FMT in ("1","3"): print(f"   截图: {SCREENSHOT_DIR}")
    if FMT in ("2","3"): print(f"   PDF:  {PDF_DIR}")
    if xlsx_path: print(f"   Excel: {xlsx_path}")
    if not all_ok:
        print(f"\n   ⚠ 部分文件仍有问题，请手动检查")


if __name__ == "__main__":
    asyncio.run(main())
