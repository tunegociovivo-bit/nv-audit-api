"""
NV Audit API v4.0 â Backend completo
Features: Ahrefs, PageSpeed completo, Content Analysis IA, GBP, robots/sitemap,
           cache SQLite, white-label PDF, radar chart data
Deploy: Railway
"""

import os, re, io, json, time, logging, sqlite3
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree
from html import escape as html_escape

import requests
from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak
)

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nv-audit")

API_TOKEN    = os.environ.get("API_TOKEN", "")
AHREFS_TOKEN = os.environ.get("AHREFS_TOKEN", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
PSI_KEY      = os.environ.get("PSI_API_KEY", "")
GPLACES_KEY  = os.environ.get("GOOGLE_PLACES_KEY", "")
CACHE_DAYS   = int(os.environ.get("CACHE_DAYS", "7"))
AHREFS_BASE  = "https://api.ahrefs.com/v3"

# Colors
C_BLACK  = HexColor("#1a1a1a")
C_DARK   = HexColor("#2a2a2a")
C_ORANGE = HexColor("#E07828")
C_WHITE  = HexColor("#f0f0f0")
C_GRAY   = HexColor("#888888")
C_GREEN  = HexColor("#34d399")
C_RED    = HexColor("#f87171")

# ââ Cache (SQLite) ââââââââââââââââââââââââââââââââââââââââââââââ
# Railway: mount a persistent volume at /data, or falls back to /tmp (ephemeral)
DB_PATH = os.environ.get("CACHE_DB", "/data/nv_audit_cache.db")
if not os.path.isdir(os.path.dirname(DB_PATH)):
    DB_PATH = "/tmp/nv_audit_cache.db"

# Simple rate limiting: max audits per hour
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_HOUR", "30"))
_rate_counter = {"count": 0, "reset_at": 0}

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.execute("""CREATE TABLE IF NOT EXISTS cache (
            domain TEXT PRIMARY KEY, data TEXT, created_at REAL
        )""")
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db: db.close()

def cache_get(domain):
    try:
        db = get_db()
        row = db.execute("SELECT data, created_at FROM cache WHERE domain=?", (domain,)).fetchone()
        if row and (time.time() - row[1]) < CACHE_DAYS * 86400:
            return json.loads(row[0])
    except: pass
    return None

def cache_set(domain, data):
    try:
        db = get_db()
        db.execute("INSERT OR REPLACE INTO cache (domain, data, created_at) VALUES (?,?,?)",
                   (domain, json.dumps(data, ensure_ascii=False), time.time()))
        db.commit()
    except Exception as e:
        log.warning(f"Cache write failed: {e}")


# ââ Helpers âââââââââââââââââââââââââââââââââââââââââââââââââââââ
def check_rate_limit():
    """Simple in-memory rate limiting."""
    now = time.time()
    if now > _rate_counter["reset_at"]:
        _rate_counter["count"] = 0
        _rate_counter["reset_at"] = now + 3600
    _rate_counter["count"] += 1
    return _rate_counter["count"] <= RATE_LIMIT

def rl_esc(text):
    """Escape text for ReportLab Paragraph (XML-safe)."""
    if not text: return ""
    return html_escape(str(text), quote=False)

def clean_domain(url):
    if not url.startswith("http"): url = "https://" + url
    return urlparse(url).netloc.lower().replace("www.", "")

def clean_url(url):
    return url if url.startswith("http") else "https://" + url

def safe_get(url, params=None, headers=None, timeout=15):
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"GET failed: {url} â {e}")
        return None

def fmt_num(n):
    if n is None: return "â"
    n = int(n) if isinstance(n, (int, float)) else 0
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(n)


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# DATA COLLECTION MODULES
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

# ââ 1. Ahrefs âââââââââââââââââââââââââââââââââââââââââââââââââââ
def _ahrefs(endpoint, domain, extra_params=None):
    today = time.strftime("%Y-%m-%d")
    params = {"target": domain, "output": "json", "mode": "subdomains", "date": today}
    if extra_params: params.update(extra_params)
    r = safe_get(f"{AHREFS_BASE}/site-explorer/{endpoint}",
        params=params, headers={"Authorization": f"Bearer {AHREFS_TOKEN}"})
    return r.json() if r else {}

def fetch_ahrefs_overview(domain):
    d = {}
    j = _ahrefs("domain-rating", domain)
    dr = j.get("domain_rating", {})
    d["domain_rating"] = dr.get("domain_rating")
    d["ahrefs_rank"] = dr.get("ahrefs_rank")

    j = _ahrefs("backlinks-stats", domain)
    m = j.get("metrics", {})
    d["backlinks_total"] = m.get("live", 0)
    d["referring_domains"] = m.get("live_refdomains", 0)
    d["dofollow_backlinks"] = m.get("live_dofollow", 0)
    return d

def fetch_ahrefs_organic(domain):
    j = _ahrefs("metrics", domain, {"country": "es", "select": "org_keywords,org_traffic,org_cost"})
    m = j.get("metrics", {})
    return {
        "organic_keywords": m.get("org_keywords", 0),
        "organic_traffic": m.get("org_traffic", 0),
        "organic_traffic_value": m.get("org_cost", 0),
    }

def fetch_ahrefs_top_keywords(domain, limit=10):
    j = _ahrefs("organic-keywords", domain,
        {"country": "es", "limit": limit, "order_by": "volume:desc",
         "select": "keyword,volume,best_position,sum_traffic,keyword_difficulty"})
    raw = j.get("keywords", [])
    for item in raw:
        if "best_position" in item:
            item["position"] = item.pop("best_position")
        if "sum_traffic" in item:
            item["traffic"] = item.pop("sum_traffic")
        if "keyword_difficulty" in item:
            item["difficulty"] = item.pop("keyword_difficulty")
    return raw

def fetch_ahrefs_competitors(domain, limit=5):
    j = _ahrefs("organic-competitors", domain, {"country": "es", "limit": limit, "select": "competitor_domain,keywords_common,traffic"})
    raw = j.get("competitors", [])
    for item in raw:
        if "competitor_domain" in item:
            item["domain"] = item.pop("competitor_domain")
        if "keywords_common" in item:
            item["common_keywords"] = item.pop("keywords_common")
        if "traffic" in item:
            item["organic_keywords"] = item.pop("traffic")
    return raw

def fetch_ahrefs_top_pages(domain, limit=5):
    j = _ahrefs("top-pages", domain,
        {"country": "es", "limit": limit,
         "select": "url,sum_traffic,keywords,top_keyword,top_keyword_best_position"})
    raw = j.get("pages", [])
    for item in raw:
        if "sum_traffic" in item:
            item["traffic"] = item.pop("sum_traffic")
        if "top_keyword_best_position" in item:
            item["position"] = item.pop("top_keyword_best_position")
    return raw

def fetch_ahrefs_referring_domains(domain, limit=10):
    j = _ahrefs("refdomains", domain,
        {"limit": limit, "order_by": "domain_rating:desc",
         "select": "domain,domain_rating,links_to_target,first_seen,last_seen"})
    raw = j.get("refdomains", [])
    for item in raw:
        if "links_to_target" in item:
            item["backlinks"] = item.pop("links_to_target")
        if "last_seen" in item:
            item["last_visited"] = item.pop("last_seen")
    return raw


# ââ 2. PageSpeed COMPLETO ââââââââââââââââââââââââââââââââââââââ
def fetch_pagespeed_full(url, strategy="mobile"):
    params = {"url": url, "strategy": strategy,
              "category": ["performance", "seo", "best-practices", "accessibility"]}
    if PSI_KEY: params["key"] = PSI_KEY
    r = safe_get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
                 params=params, timeout=40)
    if not r: return {}
    j = r.json()
    lhr = j.get("lighthouseResult", {})
    cats = lhr.get("categories", {})
    audits = lhr.get("audits", {})

    scores = {}
    for k in ["performance", "seo", "best-practices", "accessibility"]:
        scores[k.replace("-", "_")] = int((cats.get(k, {}).get("score") or 0) * 100)

    cwv = {}
    cwv_keys = {
        "largest-contentful-paint": "LCP",
        "total-blocking-time": "TBT",
        "cumulative-layout-shift": "CLS",
        "first-contentful-paint": "FCP",
        "speed-index": "SI",
        "server-response-time": "TTFB",
        "interactive": "TTI",
    }
    for audit_key, label in cwv_keys.items():
        a = audits.get(audit_key, {})
        cwv[label] = {
            "value": a.get("displayValue", "N/A"),
            "score": a.get("score"),
            "numeric": a.get("numericValue"),
        }

    # ALL failed/warning audits grouped by category
    failed = []
    opportunities = []
    diagnostics = []
    for key, audit in audits.items():
        score = audit.get("score")
        if score is not None and score < 0.9 and audit.get("title"):
            entry = {
                "id": key,
                "title": audit["title"],
                "description": (audit.get("description") or "")[:300],
                "score": score,
                "displayValue": audit.get("displayValue"),
            }
            if audit.get("details", {}).get("type") == "opportunity":
                opportunities.append(entry)
            elif score < 0.5:
                failed.append(entry)
            else:
                diagnostics.append(entry)

    return {
        "strategy": strategy,
        "scores": scores,
        "core_web_vitals": cwv,
        "failed_audits": sorted(failed, key=lambda x: x["score"])[:15],
        "opportunities": sorted(opportunities, key=lambda x: x["score"])[:10],
        "diagnostics": diagnostics[:10],
    }


# ââ 3. HTML Scraping + On-Page âââââââââââââââââââââââââââââââââ
def fetch_onpage_seo(url):
    r = safe_get(url, timeout=15)
    if not r: return {"error": "No se pudo acceder"}
    html = r.text
    d = {}

    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    d["title"] = m.group(1).strip() if m else None
    d["title_length"] = len(d["title"]) if d["title"] else 0

    for attr in ["description", "keywords"]:
        m = re.search(rf'<meta\s+name=["\']({attr})["\'][^>]*content=["\'](.*?)["\']', html, re.I)
        if not m: m = re.search(rf'<meta\s+content=["\'](.*?)["\']\s+name=["\']({attr})["\']', html, re.I)
        val = (m.group(2) if m and m.lastindex >= 2 else (m.group(1) if m else None))
        d[f"meta_{attr}"] = val.strip() if val else None
    d["meta_description_length"] = len(d["meta_description"]) if d.get("meta_description") else 0

    m = re.search(r'<link\s+rel=["\']canonical["\']\s+href=["\'](.*?)["\']', html, re.I)
    d["canonical"] = m.group(1).strip() if m else None

    headings = {}
    for lv in range(1, 7):
        found = re.findall(rf"<h{lv}[^>]*>(.*?)</h{lv}>", html, re.I | re.S)
        cleaned = [re.sub(r"<[^>]+>", "", h).strip() for h in found]
        if cleaned: headings[f"h{lv}"] = cleaned[:5]
    d["headings"] = headings
    d["h1_count"] = len(headings.get("h1", []))

    imgs = re.findall(r'<img\s[^>]*>', html, re.I)
    d["images_total"] = len(imgs)
    d["images_without_alt"] = sum(1 for i in imgs if not re.search(r'alt=["\'][^"\']+["\']', i, re.I))

    links = re.findall(r'<a\s[^>]*href=["\']([^"\'#][^"\']*)["\']', html, re.I)
    domain = clean_domain(url)
    d["internal_links"] = len([l for l in links if domain in l or l.startswith("/")])
    d["external_links"] = len([l for l in links if l.startswith("http") and domain not in l])

    schemas = re.findall(r'<script\s+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.I | re.S)
    types = []
    for s in schemas:
        try:
            j = json.loads(s)
            if isinstance(j, dict): types.append(j.get("@type", "?"))
            elif isinstance(j, list):
                for i in j:
                    if isinstance(i, dict): types.append(i.get("@type", "?"))
        except: pass
    d["schema_types"] = types
    d["has_schema"] = len(types) > 0

    social_patterns = {
        "facebook": r'https?://(?:www\.)?facebook\.com/[^\s"\'<>]+',
        "instagram": r'https?://(?:www\.)?instagram\.com/[^\s"\'<>]+',
        "twitter": r'https?://(?:www\.)?(?:twitter|x)\.com/[^\s"\'<>]+',
        "linkedin": r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[^\s"\'<>]+',
        "youtube": r'https?://(?:www\.)?youtube\.com/(?:channel|c|user|@)[^\s"\'<>]+',
        "tiktok": r'https?://(?:www\.)?tiktok\.com/@[^\s"\'<>]+',
        "pinterest": r'https?://(?:www\.)?pinterest\.\w+/[^\s"\'<>]+',
    }
    soc = {}
    for p, pat in social_patterns.items():
        mm = re.findall(pat, html, re.I)
        if mm: soc[p] = list(set(m.rstrip('/"') for m in mm))[0]
    d["social_links"] = soc
    d["social_platforms_found"] = len(soc)

    techs = []
    checks = [("wp-content|wordpress", "WordPress"), ("Shopify", "Shopify"),
              ("wix\\.com", "Wix"), ("squarespace", "Squarespace"),
              ("gtag|google-analytics|googletagmanager", "Google Analytics/GTM"),
              ("fbq\\(|facebook.*pixel", "Facebook Pixel"),
              ("hotjar", "Hotjar"), ("cookiebot|cookie-consent|gdpr", "GDPR/Cookies")]
    for pat, name in checks:
        if re.search(pat, html, re.I): techs.append(name)
    d["technologies"] = techs
    d["has_viewport"] = bool(re.search(r'<meta\s+name=["\']viewport["\']', html, re.I))
    d["is_https"] = url.startswith("https")

    og = {}
    for prop in ["og:title", "og:description", "og:image"]:
        m = re.search(rf'<meta\s+(?:property|name)=["\']({re.escape(prop)})["\'][^>]*content=["\'](.*?)["\']', html, re.I)
        if m: og[prop] = m.group(2).strip()
    d["open_graph"] = og if og else None

    tc = {}
    for prop in ["twitter:card", "twitter:site"]:
        m = re.search(rf'<meta\s+(?:property|name)=["\']({re.escape(prop)})["\'][^>]*content=["\'](.*?)["\']', html, re.I)
        if m: tc[prop] = m.group(2).strip()
    d["twitter_card"] = tc if tc else None

    hreflangs = re.findall(r'<link\s+rel=["\']alternate["\']\s+hreflang=["\']([^"\']+)["\']', html, re.I)
    d["hreflang"] = hreflangs if hreflangs else None

    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.I | re.S)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.I | re.S)
    text = re.sub(r'<[^>]+>', ' ', text)
    d["word_count"] = len(text.split())
    d["_html"] = html  # Keep for content analysis (not sent to client)

    return d


# ââ 4. Security Headers ââââââââââââââââââââââââââââââââââââââââ
def check_security_headers(url):
    r = safe_get(url, timeout=10)
    if not r: return {}
    h = {k.lower(): v for k, v in r.headers.items()}
    return {k: h.get(k) for k in [
        "x-frame-options", "strict-transport-security",
        "content-security-policy", "x-content-type-options",
        "referrer-policy", "permissions-policy"
    ]}


# ââ 5. Robots.txt + Sitemap.xml ââââââââââââââââââââââââââââââââ
def fetch_robots_sitemap(url):
    domain_url = re.match(r'(https?://[^/]+)', url)
    if not domain_url: return {}
    base = domain_url.group(1)
    result = {}

    # Robots.txt
    r = safe_get(f"{base}/robots.txt", timeout=10)
    if r and r.status_code == 200:
        txt = r.text[:3000]
        result["robots_txt"] = {
            "exists": True,
            "content_preview": txt[:500],
            "has_sitemap_ref": "sitemap" in txt.lower(),
            "has_disallow": "disallow" in txt.lower(),
            "user_agents": list(set(re.findall(r'User-agent:\s*(.+)', txt, re.I))),
            "disallow_rules": re.findall(r'Disallow:\s*(.+)', txt, re.I)[:20],
            "sitemap_urls": re.findall(r'Sitemap:\s*(\S+)', txt, re.I),
        }
    else:
        result["robots_txt"] = {"exists": False}

    # Sitemap.xml
    sitemap_urls_to_try = [f"{base}/sitemap.xml", f"{base}/sitemap_index.xml"]
    if result.get("robots_txt", {}).get("sitemap_urls"):
        sitemap_urls_to_try = result["robots_txt"]["sitemap_urls"] + sitemap_urls_to_try

    sitemap_found = False
    for surl in sitemap_urls_to_try[:3]:
        r = safe_get(surl, timeout=10)
        if r and r.status_code == 200 and ("</urlset>" in r.text or "</sitemapindex>" in r.text):
            sitemap_found = True
            try:
                root = ElementTree.fromstring(r.text[:50000])
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                urls = root.findall(".//sm:url/sm:loc", ns) or root.findall(".//sm:sitemap/sm:loc", ns)
                url_count = len(urls)
                sample = [u.text for u in urls[:10]]
                is_index = "</sitemapindex>" in r.text

                result["sitemap"] = {
                    "exists": True,
                    "url": surl,
                    "is_index": is_index,
                    "url_count": url_count,
                    "sample_urls": sample,
                }
            except:
                result["sitemap"] = {"exists": True, "url": surl, "parse_error": True}
            break

    if not sitemap_found:
        result["sitemap"] = {"exists": False}

    return result


# ââ 6. Google Business Profile ââââââââââââââââââââââââââââââââââ
def fetch_gbp_data(domain):
    if not GPLACES_KEY:
        return {"available": False, "reason": "Google Places API key not configured"}

    # Search for the business
    r = safe_get("https://maps.googleapis.com/maps/api/place/textsearch/json",
        params={"query": domain, "key": GPLACES_KEY}, timeout=15)
    if not r: return {"available": False, "reason": "API error"}

    j = r.json()
    results = j.get("results", [])
    if not results:
        return {"available": False, "reason": "No Google Business Profile found"}

    place = results[0]
    place_id = place.get("place_id")

    # Get details
    detail = {}
    if place_id:
        r2 = safe_get("https://maps.googleapis.com/maps/api/place/details/json",
            params={"place_id": place_id, "key": GPLACES_KEY,
                    "fields": "name,rating,user_ratings_total,formatted_address,formatted_phone_number,website,opening_hours,types,business_status,photos,reviews"},
            timeout=15)
        if r2:
            detail = r2.json().get("result", {})

    reviews = detail.get("reviews", [])
    review_summary = []
    for rv in reviews[:5]:
        review_summary.append({
            "rating": rv.get("rating"),
            "text": (rv.get("text") or "")[:200],
            "time": rv.get("relative_time_description"),
            "author": rv.get("author_name"),
        })

    return {
        "available": True,
        "name": detail.get("name") or place.get("name"),
        "rating": detail.get("rating") or place.get("rating"),
        "total_reviews": detail.get("user_ratings_total") or place.get("user_ratings_total", 0),
        "address": detail.get("formatted_address") or place.get("formatted_address"),
        "phone": detail.get("formatted_phone_number"),
        "website": detail.get("website"),
        "business_status": detail.get("business_status") or place.get("business_status"),
        "types": (detail.get("types") or place.get("types", []))[:5],
        "has_opening_hours": bool(detail.get("opening_hours")),
        "is_open_now": detail.get("opening_hours", {}).get("open_now"),
        "photo_count": len(detail.get("photos", [])),
        "recent_reviews": review_summary,
    }


# ââ 7. Content Analysis (IA) âââââââââââââââââââââââââââââââââââ
def fetch_content_analysis(url, onpage_data, page_html=None):
    if not OPENAI_KEY: return {"error": "OpenAI not configured"}

    # Use pre-fetched HTML if available, otherwise fetch
    html = page_html
    if not html:
        r = safe_get(url, timeout=15)
        if not r: return {"error": "Could not fetch page"}
        html = r.text

    # Extract visible text
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.I | re.S)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.I | re.S)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()[:4000]

    prompt = f"""Analiza el contenido de esta web y devuelve SOLO JSON:
URL: {url}
Title: {onpage_data.get('title', 'N/A')}
H1: {onpage_data.get('headings', {}).get('h1', ['N/A'])}
Word count: {onpage_data.get('word_count', 0)}
Texto visible (primeros 4000 chars):
{text}

JSON requerido:
{{
  "content_score": <1-100>,
  "readability": "fÃ¡cil|medio|difÃ­cil",
  "tone": "<tono detectado>",
  "primary_topic": "<tema principal>",
  "target_audience": "<audiencia objetivo>",
  "keyword_density": [{{"keyword":"...","count":N,"density":"X%"}}],
  "content_gaps": ["<contenido que falta>"],
  "strengths": ["<punto fuerte>"],
  "weaknesses": ["<punto dÃ©bil>"],
  "recommendations": ["<recomendaciÃ³n>"],
  "estimated_reading_time": "<X min>",
  "has_cta": true/false,
  "cta_quality": "buena|mejorable|ausente",
  "duplicate_risk": "bajo|medio|alto",
  "seo_content_alignment": "<anÃ¡lisis de alineaciÃ³n SEO>"
}}"""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
            json={"model": "gpt-4o", "temperature": 0.2, "max_tokens": 2000,
                  "messages": [
                      {"role": "system", "content": "Eres experto en content marketing y SEO. Responde SOLO con JSON vÃ¡lido, sin markdown."},
                      {"role": "user", "content": prompt}]},
            timeout=45)
        content = r.json()["choices"][0]["message"]["content"].strip()
        content = re.sub(r'^```\w*\n?', '', content)
        content = re.sub(r'\n?```$', '', content)
        return json.loads(content)
    except Exception as e:
        return {"error": str(e)}


# ââ 8. Radar Chart Data ââââââââââââââââââââââââââââââââââââââââ
def compute_radar_data(results):
    """Compute normalized 0-100 scores for radar chart."""
    ahrefs = {**results.get("ahrefs_overview", {}), **results.get("ahrefs_organic", {})}
    psi = results.get("pagespeed_mobile", {}).get("scores", {})
    onpage = results.get("onpage", {})
    content = results.get("content_analysis", {})
    robots = results.get("robots_sitemap", {})
    gbp = results.get("gbp", {})

    dr = ahrefs.get("domain_rating") or 0
    backlinks_score = min(100, (ahrefs.get("referring_domains") or 0) / 5)

    onpage_checks = [
        onpage.get("title") is not None,
        onpage.get("meta_description") is not None,
        onpage.get("h1_count") == 1,
        onpage.get("canonical") is not None,
        onpage.get("is_https", False),
        onpage.get("has_viewport", False),
        onpage.get("has_schema", False),
        onpage.get("open_graph") is not None,
        onpage.get("images_without_alt", 1) == 0,
    ]
    onpage_score = int(sum(onpage_checks) / len(onpage_checks) * 100) if onpage_checks else 0

    social_score = int(onpage.get("social_platforms_found", 0) / 7 * 100)

    technical_checks = [
        robots.get("robots_txt", {}).get("exists", False),
        robots.get("sitemap", {}).get("exists", False),
        onpage.get("is_https", False),
        psi.get("performance", 0) >= 50,
        psi.get("accessibility", 0) >= 70,
    ]
    tech_score = int(sum(technical_checks) / len(technical_checks) * 100)

    return {
        "labels": ["Autoridad", "Rendimiento", "SEO On-Page", "Contenido", "Social", "TÃ©cnico"],
        "values": [
            min(100, int(dr)),
            psi.get("performance", 0),
            onpage_score,
            content.get("content_score", 50) if isinstance(content, dict) else 50,
            social_score,
            tech_score,
        ],
    }


# ââ 9. GPT-4o Main Report ââââââââââââââââââââââââââââââââââââââ
def generate_ai_report(all_data):
    if not OPENAI_KEY: return {"error": "OpenAI not configured"}
    system = """Eres consultor SEO senior de Negocio Vivo. Recibes datos REALES.
Responde SOLO JSON:
{
  "score_global": <1-100>,
  "resumen_ejecutivo": "<3-4 frases>",
  "fortalezas": ["..."],
  "problemas_criticos": [{"titulo":"...","impacto":"alto|medio|bajo","solucion":"..."}],
  "quick_wins": [{"accion":"...","impacto_estimado":"...","dificultad":"fÃ¡cil|media|difÃ­cil"}],
  "analisis_competencia": "<pÃ¡rrafo>",
  "analisis_redes_sociales": "<pÃ¡rrafo>",
  "analisis_gbp": "<pÃ¡rrafo sobre Google Business Profile>",
  "plan_accion_30_60_90": {"dias_30":["..."],"dias_60":["..."],"dias_90":["..."]},
  "cta_final": "<frase invitando a contactar>"
}"""
    try:
        # Reduce payload size for GPT
        slim = {k: v for k, v in all_data.items()
                if k not in ("content_analysis_raw",)}
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
            json={"model": "gpt-4o", "temperature": 0.3, "max_tokens": 3000,
                  "messages": [{"role": "system", "content": system},
                               {"role": "user", "content": json.dumps(slim, ensure_ascii=False)[:12000]}]},
            timeout=60)
        content = r.json()["choices"][0]["message"]["content"].strip()
        content = re.sub(r'^```\w*\n?', '', content)
        content = re.sub(r'\n?```$', '', content)
        return json.loads(content)
    except Exception as e:
        return {"error": str(e)}


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# PDF GENERATION (White-Label)
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def _pdf_header(canvas, doc, domain, logo_url=None, brand_name=None):
    w, h = A4
    canvas.setFillColor(C_BLACK)
    canvas.rect(0, h - 20*mm, w, 20*mm, fill=1, stroke=0)
    canvas.setStrokeColor(C_ORANGE); canvas.setLineWidth(2)
    canvas.line(0, h - 20*mm, w, h - 20*mm)

    # Logo or brand name
    x_logo = 15*mm
    if logo_url:
        try:
            resp = requests.get(logo_url, timeout=5)
            if resp.status_code == 200:
                from reportlab.lib.utils import ImageReader
                img = ImageReader(io.BytesIO(resp.content))
                canvas.drawImage(img, x_logo, h - 18*mm, width=28*mm, height=14*mm,
                                preserveAspectRatio=True, mask='auto')
                x_logo = 46*mm
        except: pass

    name = brand_name or "NEGOCIO VIVO"
    canvas.setFillColor(C_ORANGE); canvas.setFont("Helvetica-Bold", 13)
    canvas.drawString(x_logo, h - 14*mm, name)

    canvas.setFillColor(C_WHITE); canvas.setFont("Helvetica", 9)
    canvas.drawRightString(w - 15*mm, h - 11*mm, f"AuditorÃ­a SEO â {domain}")
    canvas.drawRightString(w - 15*mm, h - 15.5*mm, time.strftime("%d/%m/%Y"))

    canvas.setFillColor(C_GRAY); canvas.setFont("Helvetica", 7)
    canvas.drawString(15*mm, 10*mm, brand_name or "negociovivo.com")
    canvas.drawRightString(w - 15*mm, 10*mm, f"PÃ¡gina {doc.page}")


def generate_pdf(data, logo_url=None, brand_name=None):
    buf = io.BytesIO()
    domain = data.get("domain", "audit")
    ai = data.get("ai_report", {})
    ah = {**data.get("ahrefs_overview", {}), **data.get("ahrefs_organic", {})}
    psi_m = data.get("pagespeed_mobile", {})
    onpage = data.get("onpage", {})
    content = data.get("content_analysis", {})
    gbp = data.get("gbp", {})
    robots = data.get("robots_sitemap", {})
    radar = data.get("radar_data", {})

    doc = SimpleDocTemplate(buf, pagesize=A4,
        topMargin=28*mm, bottomMargin=20*mm, leftMargin=15*mm, rightMargin=15*mm)

    # Styles
    sT = ParagraphStyle("T", fontName="Helvetica-Bold", fontSize=22, textColor=C_BLACK, spaceAfter=4*mm, leading=26)
    sH2 = ParagraphStyle("H2", fontName="Helvetica-Bold", fontSize=14, textColor=C_ORANGE, spaceBefore=6*mm, spaceAfter=3*mm)
    sH3 = ParagraphStyle("H3", fontName="Helvetica-Bold", fontSize=11, textColor=C_BLACK, spaceBefore=4*mm, spaceAfter=2*mm)
    sB = ParagraphStyle("B", fontName="Helvetica", fontSize=10, textColor=C_BLACK, leading=14, spaceAfter=2*mm)
    sS = ParagraphStyle("S", fontName="Helvetica", fontSize=9, textColor=C_GRAY, leading=12, spaceAfter=1*mm)
    sScore = ParagraphStyle("Sc", fontName="Helvetica-Bold", fontSize=48, textColor=C_ORANGE, alignment=TA_CENTER)
    sC = ParagraphStyle("C", fontName="Helvetica", fontSize=10, textColor=C_BLACK, alignment=TA_CENTER, leading=14)

    def make_table(headers, rows, col_widths):
        data_t = [headers] + rows
        t = Table(data_t, colWidths=col_widths)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), C_BLACK),
            ("TEXTCOLOR", (0,0), (-1,0), C_WHITE),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,-1), 9),
            ("ALIGN", (1,0), (-1,-1), "CENTER"),
            ("GRID", (0,0), (-1,-1), 0.4, HexColor("#dddddd")),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [HexColor("#ffffff"), HexColor("#f5f5f5")]),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6),
            ("TOPPADDING", (0,0), (-1,-1), 6),
        ]))
        return t

    story = []

    # ââ Page 1: Score + Resumen ââ
    story.append(Spacer(1, 8*mm))
    story.append(Paragraph("AuditorÃ­a SEO Profesional", sT))
    story.append(Paragraph(f"<b>{domain}</b>", ParagraphStyle("D", fontName="Helvetica-Bold", fontSize=14, textColor=C_ORANGE, spaceAfter=6*mm)))
    story.append(Paragraph(f"{ai.get('score_global', 0)}/100", sScore))
    story.append(Paragraph("PuntuaciÃ³n Global", sC))
    story.append(Spacer(1, 4*mm))
    if ai.get("resumen_ejecutivo"):
        story.append(Paragraph(rl_esc(ai["resumen_ejecutivo"]), sB))

    # Metrics
    story.append(Spacer(1, 3*mm))
    m_data = [["DR", "Keywords", "TrÃ¡fico/mes", "Backlinks", "Ref. Domains"],
              [str(ah.get("domain_rating", "â")), fmt_num(ah.get("organic_keywords")),
               fmt_num(ah.get("organic_traffic")), fmt_num(ah.get("backlinks_total")),
               fmt_num(ah.get("referring_domains"))]]
    t = Table(m_data, colWidths=[36*mm]*5)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), C_BLACK), ("TEXTCOLOR", (0,0), (-1,0), C_WHITE),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"), ("FONTSIZE", (0,0), (-1,0), 8),
        ("FONTNAME", (0,1), (-1,1), "Helvetica-Bold"), ("FONTSIZE", (0,1), (-1,1), 14),
        ("TEXTCOLOR", (0,1), (-1,1), C_ORANGE), ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ("GRID", (0,0), (-1,-1), 0.5, C_DARK), ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING", (0,0), (-1,-1), 8),
    ]))
    story.append(t)

    # Radar scores
    if radar.get("labels"):
        story.append(Spacer(1, 4*mm))
        r_data = [radar["labels"], [str(v) for v in radar.get("values", [])]]
        rt = Table(r_data, colWidths=[30*mm]*len(radar["labels"]))
        rt.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), HexColor("#333333")), ("TEXTCOLOR", (0,0), (-1,0), C_WHITE),
            ("FONTNAME", (0,0), (-1,-1), "Helvetica-Bold"), ("FONTSIZE", (0,0), (-1,-1), 8),
            ("TEXTCOLOR", (0,1), (-1,1), C_ORANGE), ("FONTSIZE", (0,1), (-1,1), 12),
            ("ALIGN", (0,0), (-1,-1), "CENTER"), ("GRID", (0,0), (-1,-1), 0.5, C_DARK),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6), ("TOPPADDING", (0,0), (-1,-1), 6),
        ]))
        story.append(rt)

    # Fortalezas + Problemas
    if ai.get("fortalezas"):
        story.append(Paragraph("Fortalezas", sH2))
        for f in ai["fortalezas"]:
            story.append(Paragraph(f"<font color='#34d399'>â</font>  {rl_esc(f)}", sB))
    if ai.get("problemas_criticos"):
        story.append(Paragraph("Problemas Detectados", sH2))
        for p in ai["problemas_criticos"]:
            imp = (p.get("impacto") or "medio").upper()
            c = "#f87171" if imp == "ALTO" else "#fbbf24" if imp == "MEDIO" else "#60a5fa"
            story.append(Paragraph(f"<font color='{c}'><b>[{imp}]</b></font>  <b>{rl_esc(p.get('titulo',''))}</b>", sB))
            if p.get("solucion"): story.append(Paragraph(f"    â {rl_esc(p['solucion'])}", sS))

    story.append(PageBreak())

    # ââ Page 2: Keywords + Competitors ââ
    kws = data.get("ahrefs_keywords", [])
    if kws:
        story.append(Paragraph("Top Keywords OrgÃ¡nicos", sH2))
        rows = [[rl_esc(k.get("keyword",""))[:35], str(k.get("position","â")), fmt_num(k.get("volume")),
                 fmt_num(k.get("traffic")), str(k.get("difficulty","â"))] for k in kws]
        story.append(make_table(["Keyword","Pos.","Vol.","TrÃ¡fico","KD"], rows,
                               [55*mm,18*mm,25*mm,25*mm,18*mm]))

    comps = data.get("ahrefs_competitors", [])
    if comps:
        story.append(Paragraph("Competidores OrgÃ¡nicos", sH2))
        rows = [[rl_esc(c.get("domain","")), fmt_num(c.get("common_keywords")),
                 fmt_num(c.get("organic_keywords"))] for c in comps]
        story.append(make_table(["Dominio","KW comunes","KW totales"], rows, [70*mm,45*mm,45*mm]))
    if ai.get("analisis_competencia"):
        story.append(Spacer(1, 2*mm)); story.append(Paragraph(rl_esc(ai["analisis_competencia"]), sB))

    story.append(PageBreak())

    # ââ Page 3: Content + Social + GBP ââ
    if isinstance(content, dict) and content.get("content_score"):
        story.append(Paragraph("AnÃ¡lisis de Contenido", sH2))
        story.append(Paragraph(f"PuntuaciÃ³n: <b>{content['content_score']}/100</b> Â· Legibilidad: <b>{rl_esc(content.get('readability','â'))}</b> Â· Lectura: <b>{rl_esc(content.get('estimated_reading_time','â'))}</b>", sB))
        if content.get("strengths"):
            for s in content["strengths"]: story.append(Paragraph(f"<font color='#34d399'>â</font> {rl_esc(s)}", sS))
        if content.get("weaknesses"):
            for w in content["weaknesses"]: story.append(Paragraph(f"<font color='#f87171'>â</font> {rl_esc(w)}", sS))
        if content.get("recommendations"):
            story.append(Paragraph("Recomendaciones:", sH3))
            for r in content["recommendations"]: story.append(Paragraph(f"â {rl_esc(r)}", sS))

    # Social
    soc = onpage.get("social_links", {})
    story.append(Paragraph("Redes Sociales", sH2))
    for p in ["facebook","instagram","twitter","linkedin","youtube","tiktok","pinterest"]:
        u = soc.get(p)
        icon = "â" if u else "â"; c = "#34d399" if u else "#f87171"
        val = rl_esc(u) if u else "No detectado"
        story.append(Paragraph(f"<font color='{c}'>{icon}</font>  <b>{p.capitalize()}</b>: {val}", sS))

    # GBP
    if gbp.get("available"):
        story.append(Paragraph("Google Business Profile", sH2))
        story.append(Paragraph(f"<b>{rl_esc(gbp.get('name',''))}</b> Â· Rating: {gbp.get('rating','â')} ({gbp.get('total_reviews',0)} reseÃ±as)", sB))
        story.append(Paragraph(f"DirecciÃ³n: {rl_esc(gbp.get('address','â'))}", sS))
        story.append(Paragraph(f"TelÃ©fono: {rl_esc(gbp.get('phone','â'))} Â· Fotos: {gbp.get('photo_count',0)} Â· Horario: {'SÃ­' if gbp.get('has_opening_hours') else 'No'}", sS))
        if ai.get("analisis_gbp"): story.append(Paragraph(rl_esc(ai["analisis_gbp"]), sB))

    story.append(PageBreak())

    # ââ Page 4: Technical + Robots + Plan ââ
    scores = psi_m.get("scores", {})
    story.append(Paragraph("Rendimiento (MÃ³vil)", sH2))
    ps = [["Performance","SEO","Accesibilidad","Best Practices"],
          [str(scores.get("performance","â")), str(scores.get("seo","â")),
           str(scores.get("accessibility","â")), str(scores.get("best_practices","â"))]]
    pt = Table(ps, colWidths=[44*mm]*4)
    pt.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),C_BLACK),("TEXTCOLOR",(0,0),(-1,0),C_WHITE),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,0),8),
        ("FONTNAME",(0,1),(-1,1),"Helvetica-Bold"),("FONTSIZE",(0,1),(-1,1),16),
        ("TEXTCOLOR",(0,1),(-1,1),C_ORANGE),("ALIGN",(0,0),(-1,-1),"CENTER"),
        ("GRID",(0,0),(-1,-1),0.5,C_DARK),("BOTTOMPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),8),
    ]))
    story.append(pt)

    # Robots + Sitemap
    rb = robots.get("robots_txt", {})
    sm = robots.get("sitemap", {})
    story.append(Paragraph("Robots.txt y Sitemap", sH2))
    story.append(Paragraph(f"robots.txt: <b>{'SÃ­' if rb.get('exists') else 'No encontrado'}</b>", sB))
    if rb.get("exists"):
        story.append(Paragraph(f"  User-agents: {rl_esc(', '.join(rb.get('user_agents',[])))}", sS))
        story.append(Paragraph(f"  Reglas Disallow: {len(rb.get('disallow_rules',[]))}", sS))
    story.append(Paragraph(f"Sitemap: <b>{'SÃ­' if sm.get('exists') else 'No encontrado'}</b>" +
                           (f" ({sm.get('url_count',0)} URLs)" if sm.get("exists") else ""), sB))

    # Quick Wins + Plan
    if ai.get("quick_wins"):
        story.append(Paragraph("Quick Wins", sH2))
        for q in ai["quick_wins"]:
            story.append(Paragraph(f"<font color='#E07828'>â</font>  <b>{rl_esc(q.get('accion',''))}</b>", sB))
            if q.get("impacto_estimado"): story.append(Paragraph(f"    Impacto: {rl_esc(q['impacto_estimado'])}", sS))

    if ai.get("plan_accion_30_60_90"):
        story.append(Paragraph("Plan 30-60-90 DÃ­as", sH2))
        for label, key in [("30 dÃ­as","dias_30"),("60 dÃ­as","dias_60"),("90 dÃ­as","dias_90")]:
            items = ai["plan_accion_30_60_90"].get(key, [])
            if items:
                story.append(Paragraph(f"<b>{label}</b>", sH3))
                for i in items: story.append(Paragraph(f"â {rl_esc(i)}", sB))

    # CTA
    story.append(Spacer(1, 6*mm))
    story.append(HRFlowable(width="100%", color=C_ORANGE, thickness=2))
    story.append(Spacer(1, 3*mm))
    if ai.get("cta_final"):
        story.append(Paragraph(rl_esc(ai["cta_final"]), ParagraphStyle("CTA", fontName="Helvetica-Bold",
            fontSize=12, textColor=C_ORANGE, alignment=TA_CENTER, spaceAfter=3*mm)))
    story.append(Paragraph(brand_name or "negociovivo.com Â· info@negociovivo.com", sC))

    doc.build(story,
        onFirstPage=lambda c, d: _pdf_header(c, d, domain, logo_url, brand_name),
        onLaterPages=lambda c, d: _pdf_header(c, d, domain, logo_url, brand_name))
    buf.seek(0)
    return buf


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# ENDPOINTS
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/audit", methods=["POST"])
def run_audit():
    auth = request.headers.get("Authorization", "")
    if API_TOKEN and auth != f"Bearer {API_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    url = body.get("url", "").strip()
    force = body.get("force", False)
    if not url: return jsonify({"error": "URL requerida"}), 400

    url = clean_url(url)
    domain = clean_domain(url)

    # Check cache
    if not force:
        cached = cache_get(domain)
        if cached:
            cached["from_cache"] = True
            return jsonify(cached)

    # Rate limit
    if not check_rate_limit():
        return jsonify({"error": "Rate limit exceeded. Intenta en una hora."}), 429

    log.info(f"Audit: {domain}")
    t0 = time.time()
    results = {"domain": domain, "url": url, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")}

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {}
        if AHREFS_TOKEN:
            futures["ahrefs_overview"] = ex.submit(fetch_ahrefs_overview, domain)
            futures["ahrefs_organic"] = ex.submit(fetch_ahrefs_organic, domain)
            futures["ahrefs_keywords"] = ex.submit(fetch_ahrefs_top_keywords, domain)
            futures["ahrefs_competitors"] = ex.submit(fetch_ahrefs_competitors, domain)
            futures["ahrefs_top_pages"] = ex.submit(fetch_ahrefs_top_pages, domain)
            futures["ahrefs_refdomains"] = ex.submit(fetch_ahrefs_referring_domains, domain)
        futures["pagespeed_mobile"] = ex.submit(fetch_pagespeed_full, url, "mobile")
        futures["pagespeed_desktop"] = ex.submit(fetch_pagespeed_full, url, "desktop")
        futures["onpage"] = ex.submit(fetch_onpage_seo, url)
        futures["security"] = ex.submit(check_security_headers, url)
        futures["robots_sitemap"] = ex.submit(fetch_robots_sitemap, url)
        futures["gbp"] = ex.submit(fetch_gbp_data, domain)

        for key, future in futures.items():
            try: results[key] = future.result(timeout=45)
            except Exception as e: results[key] = {"error": str(e)}

    # Content analysis â reuse HTML from onpage to avoid double fetch
    onpage = results.get("onpage", {})
    page_html = onpage.pop("_html", None)
    results["content_analysis"] = fetch_content_analysis(url, onpage, page_html)

    # AI report
    results["ai_report"] = generate_ai_report(results)

    # Radar
    results["radar_data"] = compute_radar_data(results)

    results["elapsed_seconds"] = round(time.time() - t0, 1)
    results["from_cache"] = False

    # Save to cache
    cache_set(domain, results)

    return jsonify(results)


@app.route("/audit/pdf", methods=["POST"])
def audit_pdf():
    auth = request.headers.get("Authorization", "")
    if API_TOKEN and auth != f"Bearer {API_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    if not data.get("domain"): return jsonify({"error": "Datos requeridos"}), 400
    logo_url = data.pop("_logo_url", None)
    brand_name = data.pop("_brand_name", None)
    pdf_buf = generate_pdf(data, logo_url, brand_name)
    fn = f"auditoria-seo-{data['domain'].replace('.', '-')}.pdf"
    return send_file(pdf_buf, mimetype="application/pdf", as_attachment=True, download_name=fn)


@app.route("/audit/history", methods=["GET"])
def audit_history():
    """Returns all cached audits (for history feature)."""
    auth = request.headers.get("Authorization", "")
    if API_TOKEN and auth != f"Bearer {API_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401
    try:
        db = get_db()
        rows = db.execute("SELECT domain, created_at FROM cache ORDER BY created_at DESC LIMIT 50").fetchall()
        history = []
        for domain, created_at in rows:
            full = cache_get(domain)
            if full:
                ai = full.get("ai_report", {})
                history.append({
                    "domain": domain,
                    "score": ai.get("score_global"),
                    "date": time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at)),
                    "timestamp": created_at,
                })
        return jsonify({"history": history})
    except Exception as e:
        return jsonify({"history": [], "error": str(e)})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": "4.0",
                    "ahrefs": bool(AHREFS_TOKEN), "openai": bool(OPENAI_KEY),
                    "places": bool(GPLACES_KEY)})




# ════════════════════════════════════════════════════════════
# V4 ENDPOINTS — Intake Form + Competitive Analysis
# ════════════════════════════════════════════════════════════

def fetch_competitor_ahrefs(comp_domain):
    """Fetch Ahrefs data for a single competitor."""
    try:
        overview = fetch_ahrefs_overview(comp_domain)
        organic = fetch_ahrefs_organic(comp_domain)
        keywords = fetch_ahrefs_top_keywords(comp_domain, limit=5)
        return {"domain": comp_domain, **overview, **organic, "top_keywords": keywords}
    except Exception as e:
        log.warning(f"Competitor fetch failed for {comp_domain}: {e}")
        return {"domain": comp_domain, "error": str(e)}


def compute_roi_analysis(services, results):
    """Analyze ROI per channel based on services and investment data."""
    ahrefs = {**results.get("ahrefs_overview", {}), **results.get("ahrefs_organic", {})}
    onpage = results.get("onpage", {})
    gbp = results.get("gbp", {})
    channel_scores = {}
    for svc in services:
        name = svc.get("name", "").upper()
        investment = svc.get("investment", 0) or 0
        score, metrics, recommendation = 0, {}, ""
        if name == "SEO":
            dr = ahrefs.get("domain_rating") or 0
            traffic = ahrefs.get("organic_traffic") or 0
            kws = ahrefs.get("organic_keywords") or 0
            score = min(100, int(dr * 0.4 + min(50, traffic / 100) + min(10, kws / 50)))
            metrics = {"DR": dr, "traffic": traffic, "keywords": kws}
            recommendation = "SEO necesita mejora urgente." if score < 40 else "SEO en progreso." if score < 70 else "Buen rendimiento SEO."
        elif name == "SEM":
            ppc = ahrefs.get("organic_traffic_value") or 0
            score = min(100, int(ppc / 50)) if investment > 0 else 30
            metrics = {"traffic_value": ppc, "investment": investment}
            recommendation = f"ROI estimado: {ppc/investment:.1f}x" if investment > 0 and ppc > 0 else "Sin datos SEM suficientes."
        elif name == "GMB":
            if gbp.get("available"):
                rating = gbp.get("rating") or 0
                reviews = gbp.get("total_reviews") or 0
                score = min(100, int(rating * 15 + min(25, reviews / 2)))
                metrics = {"rating": rating, "reviews": reviews}
                recommendation = "Perfil optimizado." if score >= 70 else "Mejorar reseñas y perfil GMB."
            else:
                recommendation = "No se encontró perfil GMB. Crear uno."
        elif name == "RRSS":
            sc = onpage.get("social_platforms_found", 0)
            score = min(100, sc * 20)
            metrics = {"platforms_detected": sc, "platforms": list(onpage.get("social_links", {}).keys())}
            recommendation = f"{sc} redes detectadas. " + ("Buena presencia." if sc >= 4 else "Ampliar presencia.")
        elif name == "MAILING":
            has_nl = bool(re.search(r'newsletter|mailchimp|sendinblue|mailerlite', str(onpage.get("technologies", [])), re.I))
            score = 60 if has_nl else 20
            metrics = {"newsletter_detected": has_nl}
            recommendation = "Email marketing detectado." if has_nl else "Implementar email marketing."
        elif name == "SEO IA":
            ca = results.get("content_analysis", {})
            score = ca.get("content_score", 40) if isinstance(ca, dict) else 40
            metrics = {"content_score": score}
            recommendation = "Buen contenido IA." if score >= 60 else "Mejorar contenido con IA."
        performance = "Alto" if score >= 70 else "Medio" if score >= 40 else "Bajo"
        channel_scores[name] = {"score": score, "investment": investment, "performance": performance, "metrics": metrics, "recommendation": recommendation}
    return channel_scores


def generate_v4_ai_report(all_data, client_name, services, competitors_data):
    """Enhanced AI report with competitive analysis and ROI."""
    if not OPENAI_KEY:
        return {"error": "OpenAI not configured"}
    svc_names = ", ".join(s.get("name","") for s in services)
    comp_names = ", ".join(c.get("domain","") for c in competitors_data if not c.get("error"))
    system = f"""Eres consultor de marketing digital senior de Negocio Vivo.
Analizas al cliente "{client_name}" que tiene contratados: {svc_names}.
Sus competidores son: {comp_names}.
Recibes datos REALES. Responde SOLO JSON valido (sin markdown):
{{"score_global": <1-100>, "resumen_ejecutivo": "<3-4 frases>",
"fortalezas": ["..."], "debilidades": ["..."],
"problemas_criticos": [{{"titulo":"...","impacto":"alto|medio|bajo","solucion":"..."}}],
"quick_wins": [{{"accion":"...","impacto_estimado":"...","dificultad":"facil|media|dificil"}}],
"roi_summary": "<que canales rinden mejor y donde se desperdicia inversion>",
"analisis_competencia": "<parrafo comparando con competidores>",
"ventajas_sobre_competencia": ["..."], "desventajas_vs_competencia": ["..."],
"analisis_redes_sociales": "<parrafo>", "analisis_gbp": "<parrafo>",
"plan_accion_30_60_90": {{"dias_30":["..."],"dias_60":["..."],"dias_90":["..."]}}}}"""
    try:
        slim = {k: v for k, v in all_data.items() if k not in ("content_analysis_raw",)}
        slim["competitors_summary"] = competitors_data
        slim["client_name"] = client_name
        slim["services"] = services
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
            json={"model": "gpt-4o", "temperature": 0.3, "max_tokens": 4000,
                  "messages": [{"role": "system", "content": system},
                               {"role": "user", "content": json.dumps(slim, ensure_ascii=False)[:15000]}]},
            timeout=90)
        raw = r.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r'```\w*\n?', '', raw).strip()
        return json.loads(raw)
    except Exception as e:
        return {"error": str(e)}


def compute_v4_radar(results, competitors_data):
    """Radar data including competitor comparison."""
    client_radar = compute_radar_data(results)
    comp_radars = []
    for comp in competitors_data:
        if comp.get("error"):
            continue
        dr = comp.get("domain_rating") or 0
        comp_radars.append({"domain": comp.get("domain", "?"), "values": [min(100, int(dr)), 50, 50, 50, 50, 50]})
    return {"labels": client_radar["labels"], "client_values": client_radar["values"], "competitor_radars": comp_radars}


@app.route("/audit/v4", methods=["POST"])
def run_audit_v4():
    """V4 audit: client name, services with investment, competitors."""
    auth = request.headers.get("Authorization", "")
    if API_TOKEN and auth != f"Bearer {API_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    url = body.get("url", "").strip()
    client_name = body.get("client_name", "").strip()
    services = body.get("services", [])
    competitors = body.get("competitors", [])
    force = body.get("force", False)
    if not url:
        return jsonify({"error": "URL requerida"}), 400
    url = clean_url(url)
    domain = clean_domain(url)
    if not check_rate_limit():
        return jsonify({"error": "Rate limit exceeded"}), 429
    log.info(f"V4 Audit: {domain} | Client: {client_name} | Services: {[s.get('name') for s in services]}")
    t0 = time.time()
    results = {"version": "4.0", "domain": domain, "url": url, "client_name": client_name,
               "services": services, "competitor_domains": competitors,
               "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {}
        if AHREFS_TOKEN:
            futures["ahrefs_overview"] = ex.submit(fetch_ahrefs_overview, domain)
            futures["ahrefs_organic"] = ex.submit(fetch_ahrefs_organic, domain)
            futures["ahrefs_keywords"] = ex.submit(fetch_ahrefs_top_keywords, domain)
            futures["ahrefs_competitors"] = ex.submit(fetch_ahrefs_competitors, domain)
            futures["ahrefs_top_pages"] = ex.submit(fetch_ahrefs_top_pages, domain)
            futures["ahrefs_refdomains"] = ex.submit(fetch_ahrefs_referring_domains, domain)
        futures["pagespeed_mobile"] = ex.submit(fetch_pagespeed_full, url, "mobile")
        futures["pagespeed_desktop"] = ex.submit(fetch_pagespeed_full, url, "desktop")
        futures["onpage"] = ex.submit(fetch_onpage_seo, url)
        futures["security"] = ex.submit(check_security_headers, url)
        futures["robots_sitemap"] = ex.submit(fetch_robots_sitemap, url)
        futures["gbp"] = ex.submit(fetch_gbp_data, domain)
        comp_futures = {}
        if AHREFS_TOKEN:
            for cd in competitors[:3]:
                cc = clean_domain(cd) if cd.startswith("http") else cd.lower().replace("www.", "")
                comp_futures[cc] = ex.submit(fetch_competitor_ahrefs, cc)
        for key, future in futures.items():
            try:
                results[key] = future.result(timeout=45)
            except Exception as e:
                results[key] = {"error": str(e)}
        competitors_data = []
        for cd, future in comp_futures.items():
            try:
                competitors_data.append(future.result(timeout=45))
            except Exception as e:
                competitors_data.append({"domain": cd, "error": str(e)})
    results["competitors_data"] = competitors_data
    onpage = results.get("onpage", {})
    page_html = onpage.pop("_html", None)
    results["content_analysis"] = fetch_content_analysis(url, onpage, page_html)
    results["roi_analysis"] = compute_roi_analysis(services, results)
    total_inv = sum(s.get("investment", 0) or 0 for s in services)
    results["investment_summary"] = {"total": total_inv, "by_channel": {s.get("name","?"): s.get("investment",0) for s in services}}
    results["ai_report"] = generate_v4_ai_report(results, client_name, services, competitors_data)
    results["radar_data"] = compute_v4_radar(results, competitors_data)
    results["elapsed_seconds"] = round(time.time() - t0, 1)
    cache_set(f"v4_{domain}", results)
    return jsonify(results)


@app.route("/meta/call", methods=["POST"])
def meta_call():
    """Relay authenticated requests to the Meta Graph API."""
    auth = request.headers.get("Authorization", "")
    if API_TOKEN and auth != f"Bearer {API_TOKEN}":
        log.warning("meta_call: unauthorized request")
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    method = (body.get("method") or "").strip().upper()
    url = (body.get("url") or "").strip()
    params = body.get("params") or {}
    post_body = body.get("body") or {}
    access_token = (body.get("access_token") or "").strip()

    if not method or not url or not access_token:
        missing = [f for f, v in [("method", method), ("url", url), ("access_token", access_token)] if not v]
        log.warning(f"meta_call: missing required fields: {missing}")
        return jsonify({"error": "Fields 'method', 'url', and 'access_token' are required", "missing": missing}), 400

    if not isinstance(params, dict):
        params = {}

    # Inject access token into query params
    params["access_token"] = access_token

    log.info(f"meta_call: {method} {url}")

    try:
        if method == "GET":
            r = safe_get(url, params=params, timeout=15)
            if r is None:
                log.error(f"meta_call: GET request failed for {url}")
                return jsonify({"error": "Meta API request failed or returned an error"}), 502
            try:
                return jsonify(r.json()), r.status_code
            except Exception:
                return r.text, r.status_code
        elif method == "POST":
            try:
                r = requests.post(url, params=params, json=post_body, timeout=15)
                try:
                    return jsonify(r.json()), r.status_code
                except Exception:
                    return r.text, r.status_code
            except Exception as e:
                log.error(f"meta_call: POST request failed for {url} — {e}")
                return jsonify({"error": f"Meta API POST request failed: {e}"}), 502
        else:
            log.warning(f"meta_call: unsupported method '{method}'")
            return jsonify({"error": f"Unsupported method: {method}. Use GET or POST"}), 400
    except Exception as e:
        log.error(f"meta_call: unexpected error — {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
