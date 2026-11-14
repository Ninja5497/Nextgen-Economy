#!/usr/bin/env python3
"""
NEG news fetcher. Runs on GitHub Actions every ~10 minutes.
Reads each outlet's RSS feed + Google News "site:" searches (for outlets with no
working RSS, like Reuters, and for per-category / AI coverage), classifies the
stories, and writes news.json for the website. Standard library only.
"""
import json, re, sys, time, html, hashlib, os
import urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

OUT = "news.json"
KEEP_DAYS = 3          # drop stories older than this
MAX_TOTAL = 800
MAX_PER_CAT = 160
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

# name -> (label, domain, native RSS feeds)
SOURCES = {
  "Forbes": ("Forbes", "forbes.com", [
    "https://www.forbes.com/money/feed2/", "https://www.forbes.com/business/feed2/",
    "https://www.forbes.com/investing/feed2/", "https://www.forbes.com/technology/feed2/",
    "https://www.forbes.com/healthcare/feed2/", "https://www.forbes.com/education/feed2/"]),
  "Bloomberg": ("Bloomberg", "bloomberg.com", [
    "https://feeds.bloomberg.com/markets/news.rss", "https://feeds.bloomberg.com/economics/news.rss",
    "https://feeds.bloomberg.com/technology/news.rss", "https://feeds.bloomberg.com/politics/news.rss"]),
  "WSJ": ("WSJ", "wsj.com", [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    "https://feeds.a.dj.com/rss/RSSWSJD.xml", "https://feeds.a.dj.com/rss/RSSWorldNews.xml"]),
  "FT": ("Financial Times", "ft.com", [
    "https://www.ft.com/markets?format=rss", "https://www.ft.com/companies?format=rss",
    "https://www.ft.com/technology?format=rss", "https://www.ft.com/world/economy?format=rss"]),
  "CNBC": ("CNBC", "cnbc.com", [
    "https://www.cnbc.com/id/100003114/device/rss/rss.html", "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://www.cnbc.com/id/15839135/device/rss/rss.html", "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html"]),
  "Economist": ("The Economist", "economist.com", [
    "https://www.economist.com/finance-and-economics/rss.xml", "https://www.economist.com/business/rss.xml",
    "https://www.economist.com/science-and-technology/rss.xml"]),
  "BusinessInsider": ("Business Insider", "businessinsider.com", [
    "https://feeds.businessinsider.com/custom/all", "https://markets.businessinsider.com/rss/news",
    "https://feeds.businessinsider.com/custom/tech"]),
  "MarketWatch": ("MarketWatch", "marketwatch.com", [
    "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
    "https://feeds.content.dowjones.io/public/rss/mw_bulletins"]),
  "Investopedia": ("Investopedia", "investopedia.com", [
    "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline",
    "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_news",
    "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_investing"]),
}

# Google News search terms per category (used per outlet via site:)
CAT_QUERY = {
  "AI": "artificial intelligence OR AI OR OpenAI OR chatbot OR \"machine learning\" OR robotics",
  "Sharemarket": "stocks OR markets OR Nasdaq OR \"Wall Street\" OR earnings OR IPO",
  "Technology": "technology OR semiconductor OR cybersecurity OR startup OR quantum OR \"clean energy\"",
  "Health": "health OR biotech OR drug OR \"clinical trial\" OR breakthrough OR healthcare",
  "Education": "education OR university OR edtech OR students OR \"skills\" OR workforce",
  "Business": "business OR economy OR inflation OR \"central bank\" OR growth",
}

# "Next Economy": forward-looking stories (breakthroughs, funding, emerging sectors, outlooks)
NEXT_RE = re.compile("|".join([
  "breakthrough", "discover", "emerging", "next-generation", "next generation", "the future", "future of",
  "forecast", "outlook", "projected", "predict", "set to", "will reshape", "transform", "revolution",
  "startup", "start-up", "funding round", "raises", "venture", "billion investment", "invest(?:s|ing|ment) in",
  "approval", "approves", "approved", "clinical trial", "phase 3", "gene therapy", "glp-1", "obesity drug",
  "quantum", "fusion", "robot", "humanoid", "autonomous", "agentic", "ai agent", "data center", "chip",
  "semiconductor", "battery", "clean energy", "renewable", "space", "satellite", "edtech", "reskilling",
  "workforce", "jobs of the future", "new technology", "launch", "unveil", "first-ever", "record growth",
]), re.I)

CAT_KW = {
  "AI": [r"\bai\b", r"\ba\.i\.", "artificial intelligence", "machine learning", "deep learning", "neural network",
         "openai", "chatgpt", "gpt-", "anthropic", "claude", "gemini", "copilot", "generative", "large language",
         "llm", "chatbot", "nvidia", "agentic", "ai-powered", "robotics", "autonomous", "computer vision"],
  "Health": ["health", "medical", "hospital", "pharma", "drug", "vaccine", r"\bfda\b", "clinical trial", "patient",
             "disease", "cancer", "mental health", "wellness", "nutrition", "surgery", "therapy", "pandemic",
             "virus", "biotech", "medicine", "obesity", "gene therapy", "medicare"],
  "Education": ["education", "university", "universities", "school", "student", "scholarship", "curriculum",
                "teacher", "college", "tuition", "campus", "graduate", "literacy", "e-learning", "online course"],
  "Sharemarket": ["stock", "shares", "share market", "nasdaq", r"s&p 500", "dow jones", "equity", "equities", r"\bipo\b",
                  "dividend", "bull market", "bear market", r"\betf\b", "wall street", "index fund", "earnings",
                  "crypto", "bitcoin", "ethereum", "hedge fund", "futures", "treasury yield", "rally", "sell-off",
                  "stock market", "share market", "markets"],
  "Technology": ["technology", "tech ", "software", "hardware", "cloud", "cybersecurity", "cyber", "5g", "startup",
                 "silicon valley", "semiconductor", r"\bchip", "quantum", "blockchain", "apple", "google", "microsoft",
                 "meta platforms", "amazon", "tesla", "smartphone", "data center", "satellite", "spacex"],
}
CAT_RE = {c: re.compile("|".join(k if k.startswith(r"\b") or "\\" in k else re.escape(k) for k in kws), re.I)
          for c, kws in CAT_KW.items()}
ORDER = ["AI", "Health", "Education", "Sharemarket", "Technology"]

JUNK_RE = re.compile(r"wordle|connections hints|strands hints|horoscope|crossword|sudoku|powerball|mega millions|"
                     r"about dow jones|print edition|the daily briefing|podcast|quiz\b", re.I)
EDITION_RE = re.compile(r"\|\s*[A-Z][a-z]+ \d{1,2}(st|nd|rd|th) \d{4}\s*$")
def is_junk(title):
    return bool(JUNK_RE.search(title) or EDITION_RE.search(title))

def classify(text, hint=None):
    for c in ORDER:
        if CAT_RE[c].search(text):
            return c
    return "Business"   # no clear topic keywords -> general economy/business

def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def local(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""

def child_text(el, *names):
    for c in el:
        if local(c.tag) in names and (c.text or "").strip():
            return c.text.strip()
    return ""

def strip_html(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", html.unescape(s)).strip()

def https_only(u):
    u = (u or "").strip()
    return u if u.startswith(("https://", "http://")) else ""

def find_image(el, raw_html):
    for c in el.iter():
        n = local(c.tag)
        if n in ("content", "thumbnail", "enclosure"):
            u = https_only(c.get("url") or "")
            typ = (c.get("type") or c.get("medium") or "")
            if u and (n != "enclosure" or typ.startswith("image")) and (n == "thumbnail" or "video" not in typ):
                return u
    m = re.search(r"<img[^>]+src=[\"']([^\"']+)[\"']", raw_html or "", re.I)
    return https_only(m.group(1)) if m else ""

def parse_date(s):
    if not s:
        return datetime.now(timezone.utc)
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)

def parse_feed(data, source_key, hint=None, google=False):
    label = SOURCES[source_key][0]
    root = ET.fromstring(data)
    out = []
    entries = [e for e in root.iter() if local(e.tag) in ("item", "entry")]
    for it in entries[:40]:
        title = strip_html(child_text(it, "title"))
        if google:  # Google News appends " - Outlet"
            title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title)
        link = child_text(it, "link")
        if not link:
            for c in it:
                if local(c.tag) == "link" and c.get("href"):
                    link = c.get("href"); break
        link = https_only(link or child_text(it, "guid", "id"))
        raw = child_text(it, "description", "summary", "encoded", "content")
        desc = "" if google else strip_html(raw)[:260]
        pub = child_text(it, "pubDate", "published", "updated", "date")
        if not title or not link or is_junk(title):
            continue
        date = parse_date(pub)
        uid = "n_" + hashlib.sha1((re.sub(r"\W+", "", title.lower())[:90]).encode()).hexdigest()[:16]
        text = title + " " + desc
        out.append({
            "next": bool(NEXT_RE.search(text)),
            "uid": uid, "title": title[:240],
            "description": desc or f"Read the full story at {label}.",
            "category": classify(text, hint),
            "image": "" if google else find_image(it, raw),
            "source": label, "sourceName": source_key, "sourceUrl": link,
            "date": date.isoformat(),
        })
    return out

def gnews_url(domain, cat):
    q = f"({CAT_QUERY[cat]}) site:{domain} when:2d"
    return "https://news.google.com/rss/search?q=" + urllib.parse.quote(q) + "&hl=en-US&gl=US&ceid=US:en"

def build_jobs():
    jobs = []
    for key, (label, domain, feeds) in SOURCES.items():
        for f in feeds:
            jobs.append((key, f, None, False))
        for cat in CAT_QUERY:
            if cat == "Business" and feeds:   # outlets with native RSS already cover general business
                continue
            jobs.append((key, gnews_url(domain, cat), cat, True))
    return jobs

def run_job(job):
    key, url, hint, google = job
    for attempt in range(2):
        try:
            return parse_feed(http_get(url), key, hint, google)
        except Exception as e:
            err = str(e)[:80]
            time.sleep(1.5)
    print(f"  fail {key} {'(gnews '+hint+')' if google else url[-50:]}: {err}", file=sys.stderr)
    return []

def main():
    prev = []
    if os.path.exists(OUT):
        try:
            prev = json.load(open(OUT, encoding="utf-8")).get("items", [])
        except Exception:
            prev = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(run_job, build_jobs()))
    new = [i for r in results for i in r]
    print(f"fetched {len(new)} items from {sum(1 for r in results if r)}/{len(results)} feeds")
    if not new and prev:
        print("nothing fetched; keeping existing file"); return

    cleaned = []
    for i in prev:
        if is_junk(i.get("title", "")):
            continue
        txt = i.get("title", "") + " " + i.get("description", "")
        i["category"] = classify(txt)
        i["next"] = bool(NEXT_RE.search(txt))
        cleaned.append(i)
    prev = cleaned
    merged = {}
    for i in prev + new:                      # new overwrites old (fresher image/category)
        merged[i["uid"]] = i
    cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    items = [i for i in merged.values() if parse_date(i["date"]) >= cutoff]
    items.sort(key=lambda i: i["date"], reverse=True)

    per, kept = {}, []
    for i in items:
        c = i["category"]
        if per.get(c, 0) >= MAX_PER_CAT: continue
        per[c] = per.get(c, 0) + 1
        kept.append(i)
        if len(kept) >= MAX_TOTAL: break

    old_ids = [(i["uid"], i["category"]) for i in prev]
    if [(i["uid"], i["category"]) for i in kept] == old_ids:
        print("no changes"); return
    json.dump({"updated": datetime.now(timezone.utc).isoformat(), "count": len(kept), "items": kept},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("wrote", len(kept), "items;", {c: per[c] for c in sorted(per)})

if __name__ == "__main__":
    main()
