#!/usr/bin/env python3
"""
CMS Coverage & Procedure Code Monitor
Monitors CMS for new procedure codes, coverage determinations, product approvals,
enforcement actions, and policy changes that could impact publicly traded companies.

Data Sources (22 scanners):
  Original 12: NCDs, LCDs (with token auth), MEDCAC, Tech Assessments,
                HCPCS Updates/Coding, CMS Newsroom, Federal Register, Coverage Page
  Tier 1: Drug Price Negotiation, Drug Negotiation Guidance, ASP Drug Pricing,
           MA Rate Announcements, MA Star Ratings, CMS Enforcement Actions
  Tier 2: CMMI Innovation Models, Hospital Payments (IPPS/OPPS),
           CMS Transmittals, DMEPOS Bidding

Uses DeepSeek AI with content enrichment for specific, actionable alerts.
Active 6 AM - 6 PM EST on market open days. 3-minute scan interval.
"""

import os
import sys
import json
import time
import hashlib
import logging
import signal
import threading
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ─── Configuration ────────────────────────────────────────────────────

# Discord Webhooks
ALERTS_WEBHOOK = os.getenv(
    "CMS_ALERTS_WEBHOOK",
    "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo"
)
HEALTH_WEBHOOK = os.getenv(
    "CMS_HEALTH_WEBHOOK",
    "https://discordapp.com/api/webhooks/1477100525173477593/PBPhVAR1woZSZmgNsdV5deU8l9NGVvzvQ8BhrBFxXCAvzV1JPcPurlXZBIZVje8NhPrD"
)

# DeepSeek API
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

# Timing
EST = ZoneInfo("America/New_York")
MARKET_OPEN_HOUR = int(os.getenv("CMS_MARKET_OPEN", "6"))    # 6 AM EST
MARKET_CLOSE_HOUR = int(os.getenv("CMS_MARKET_CLOSE", "18"))  # 6 PM EST
SCAN_INTERVAL_SECONDS = int(os.getenv("CMS_SCAN_INTERVAL", "180"))  # Check every 3 minutes
MAX_ITEM_AGE_DAYS = int(os.getenv("CMS_MAX_ITEM_AGE_DAYS", "7"))  # Ignore items older than 7 days

# Persistence
DATA_DIR = Path(os.getenv("CMS_DATA_DIR", "/app/data"))
SEEN_FILE = DATA_DIR / "seen_items.json"
HEALTH_FILE = DATA_DIR / "health_status.json"
LOG_FILE = DATA_DIR / "cms_monitor.log"

# CMS Coverage API Base
CMS_API_BASE = "https://api.coverage.cms.gov"

# LCD License Token (required since Coverage API v1.5)
LCD_TOKEN_CACHE = {"token": None, "expires": 0}

# ─── Logging ──────────────────────────────────────────────────────────

DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("cms_monitor")

# ─── Graceful Shutdown ────────────────────────────────────────────────

shutdown_event = threading.Event()

def handle_signal(signum, frame):
    log.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ─── Persistence ──────────────────────────────────────────────────────

def load_seen() -> dict:
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text())
        except Exception:
            log.warning("Corrupt seen file, starting fresh")
    return {}

def save_seen(seen: dict):
    SEEN_FILE.write_text(json.dumps(seen, indent=2, default=str))

def item_hash(source: str, item_id: str) -> str:
    return hashlib.md5(f"{source}:{item_id}".encode()).hexdigest()

# ─── Health Tracking ──────────────────────────────────────────────────

class HealthTracker:
    def __init__(self):
        self.status = self._load()

    def _load(self) -> dict:
        if HEALTH_FILE.exists():
            try:
                return json.loads(HEALTH_FILE.read_text())
            except Exception:
                pass
        return {
            "last_scan_time": None, "last_scan_success": None,
            "total_scans": 0, "total_errors": 0, "total_alerts_sent": 0,
            "source_status": {}, "consecutive_failures": 0,
            "last_health_report": None,
            "started_at": datetime.now(EST).isoformat(),
        }

    def save(self):
        HEALTH_FILE.write_text(json.dumps(self.status, indent=2, default=str))

    def record_scan(self, success: bool, source_results: dict):
        self.status["last_scan_time"] = datetime.now(EST).isoformat()
        self.status["last_scan_success"] = success
        self.status["total_scans"] += 1
        if not success:
            self.status["total_errors"] += 1
            self.status["consecutive_failures"] += 1
        else:
            self.status["consecutive_failures"] = 0
        for source, result in source_results.items():
            prev = self.status["source_status"].get(source, {})
            consec = prev.get("consecutive_failures", 0)
            if result.get("success", False):
                consec = 0
            else:
                consec += 1
            self.status["source_status"][source] = {
                "last_check": datetime.now(EST).isoformat(),
                "success": result.get("success", False),
                "items_found": result.get("items_found", 0),
                "error": result.get("error"),
                "consecutive_failures": consec,
            }
        self.save()

    def record_alert(self):
        self.status["total_alerts_sent"] += 1
        self.save()

    def should_alert_health(self) -> tuple[bool, str]:
        reasons = []
        if self.status["consecutive_failures"] >= 3:
            reasons.append(f"⚠️ {self.status['consecutive_failures']} consecutive scan failures")
        for source, info in self.status.get("source_status", {}).items():
            consec = info.get("consecutive_failures", 0)
            if consec >= 3:
                reasons.append(f"❌ Source `{source}` failing {consec}x in a row: {(info.get('error') or 'unknown')[:100]}")
        if self.status["last_scan_time"]:
            last_scan = datetime.fromisoformat(self.status["last_scan_time"])
            now = datetime.now(EST)
            if (now - last_scan).total_seconds() > 900 and is_market_hours():
                reasons.append(f"⏰ No scan in {int((now - last_scan).total_seconds() / 60)} minutes")
        return (len(reasons) > 0, "\n".join(reasons))

health = HealthTracker()

# ─── Market Hours ─────────────────────────────────────────────────────

MARKET_HOLIDAYS_2025 = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
}
MARKET_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}
MARKET_HOLIDAYS = MARKET_HOLIDAYS_2025 | MARKET_HOLIDAYS_2026

def is_market_day() -> bool:
    now = datetime.now(EST)
    return now.weekday() < 5 and now.strftime("%Y-%m-%d") not in MARKET_HOLIDAYS

def is_market_hours() -> bool:
    if not is_market_day():
        return False
    now = datetime.now(EST)
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR

# ─── Discord Webhook ──────────────────────────────────────────────────

def send_discord_alert(content: str, webhook_url: str = ALERTS_WEBHOOK, embed: dict = None):
    payload = {}
    if content:
        if len(content) > 2000:
            content = content[:1997] + "..."
        payload["content"] = content
    if embed:
        if embed.get("description") and len(embed["description"]) > 4096:
            embed["description"] = embed["description"][:4093] + "..."
        payload["embeds"] = [embed]
    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 5)
            log.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            requests.post(webhook_url, json=payload, timeout=15)
        elif resp.status_code not in (200, 204):
            log.error(f"Discord webhook error: {resp.status_code} - {resp.text}")
        else:
            health.record_alert()
    except Exception as e:
        log.error(f"Discord send failed: {e}")

def send_health_alert(message: str):
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "🏥 CMS Monitor Health Alert",
        "description": message, "color": 0xFF4444,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "CMS Coverage Monitor • Health Check"},
    })

def send_health_ok():
    s = health.status
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "✅ CMS Monitor Health Check - OK",
        "description": (
            f"**Uptime since:** {s.get('started_at', 'N/A')}\n"
            f"**Total scans:** {s.get('total_scans', 0)}\n"
            f"**Total errors:** {s.get('total_errors', 0)}\n"
            f"**Total alerts sent:** {s.get('total_alerts_sent', 0)}\n"
            f"**Consecutive failures:** {s.get('consecutive_failures', 0)}\n\n"
            f"**Source Status:**\n" +
            "\n".join(
                f"• `{src}`: {'✅' if info.get('success') else '❌'} "
                f"(last: {info.get('last_check', 'never')[:16]})"
                for src, info in s.get("source_status", {}).items()
            )
        ),
        "color": 0x00CC66,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "CMS Coverage Monitor • Daily Health"},
    })

# ─── CMS Data Source Scanners ─────────────────────────────────────────

class CMSScanner:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "CMSCoverageMonitor/1.0 (Financial Research)",
        "Accept": "application/json",
    })

    @staticmethod
    def safe_get(url: str, params: dict = None, timeout: int = 30, accept_json: bool = True) -> requests.Response | None:
        try:
            headers = {}
            if not accept_json:
                headers["Accept"] = "text/html"
            resp = CMSScanner.session.get(url, params=params, timeout=timeout, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as e:
            log.error(f"GET {url} failed: {e}")
            return None


# ─── LCD License Token ────────────────────────────────────────────────

def get_lcd_token() -> str | None:
    """Get a license agreement token for LCD API endpoints (valid 1 hour)."""
    now = time.time()
    if LCD_TOKEN_CACHE["token"] and now < LCD_TOKEN_CACHE["expires"]:
        return LCD_TOKEN_CACHE["token"]
    try:
        resp = requests.get(
            f"{CMS_API_BASE}/v1/metadata/license-agreement/",
            headers={"Accept": "application/json", "User-Agent": "CMSCoverageMonitor/1.0"},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"LCD license endpoint returned {resp.status_code}")
            return None
        data = resp.json()
        token = None
        if isinstance(data, str):
            token = data
        elif isinstance(data, dict):
            token = data.get("token") or data.get("access_token") or data.get("licenseToken")
            if not token:
                for v in data.values():
                    if isinstance(v, str) and len(v) > 20:
                        token = v
                        break
        if token:
            LCD_TOKEN_CACHE["token"] = token
            LCD_TOKEN_CACHE["expires"] = now + 3300  # 55 min cache
            log.info("LCD license token obtained successfully")
        return token
    except Exception as e:
        log.error(f"Failed to get LCD license token: {e}")
        return None


# ─── Original 12 Scanners ────────────────────────────────────────────

def scan_ncd_reports() -> dict:
    """Scan National Coverage Determinations (NCDs)."""
    source = "NCD Reports"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/national-coverage-ncd/")
        if resp is None:
            result["error"] = "API request failed"
            return result
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", data.get("results", []))
        for item in items[:50]:
            item_id = str(item.get("ncd_id", item.get("id", item.get("documentId", ""))))
            title = item.get("title", item.get("ncdTitle", ""))
            if not item_id and title:
                item_id = hashlib.md5(title.encode()).hexdigest()[:12]
            result["items"].append({
                "id": item_id, "title": title,
                "description": json.dumps(item)[:1000], "source": source,
                "url": f"https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid={item_id}" if item_id.isdigit() else "",
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"NCD scan error: {e}")
    return result


def scan_ncd_whats_new() -> dict:
    """Scan NCD What's New report for recent coverage document changes."""
    source = "NCD Coverage Docs"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/national-coverage-medicare-coverage-documents/")
        if resp is None:
            result["error"] = "API request failed"
            return result
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", data.get("results", []))
        for item in items[:50]:
            item_id = str(item.get("id", item.get("documentId", "")))
            title = item.get("title", item.get("documentTitle", ""))
            if not item_id and title:
                item_id = hashlib.md5(title.encode()).hexdigest()[:12]
            result["items"].append({
                "id": item_id, "title": title,
                "description": json.dumps(item)[:1000], "source": source,
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"NCD What's New scan error: {e}")
    return result


def scan_lcd_whats_new() -> dict:
    """Scan Local Coverage reports with license token auth (API v1.5)."""
    source = "LCD What's New"
    result = {"success": False, "items_found": 0, "items": []}

    token = get_lcd_token()

    # Token-auth endpoints first, then legacy reports
    endpoints = []
    if token:
        endpoints.extend([
            ("/v1/data/article/", True),
            ("/v1/data/lcd/", True),
        ])
    endpoints.extend([
        ("/v1/reports/local-coverage-whats-new/", False),
        ("/v1/reports/local-coverage-articles/", False),
    ])

    for path, needs_token in endpoints:
        try:
            headers = {"Accept": "application/json"}
            if needs_token and token:
                headers["Authorization"] = f"Bearer {token}"

            resp = CMSScanner.session.get(
                f"{CMS_API_BASE}{path}", timeout=15, headers=headers
            )
            if resp.status_code != 200:
                log.debug(f"LCD endpoint {path} returned {resp.status_code}")
                continue

            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", data.get("results", []))
            if not items and isinstance(data, dict):
                for key in data:
                    if isinstance(data[key], list) and len(data[key]) > 0:
                        items = data[key]
                        break

            for item in (items or [])[:100]:
                item_id = str(item.get("id", item.get("documentId", "")))
                title = item.get("title", item.get("documentTitle", ""))
                doc_type = item.get("documentType", item.get("type", ""))
                if not item_id and title:
                    item_id = hashlib.md5(title.encode()).hexdigest()[:12]
                if not item_id:
                    continue
                result["items"].append({
                    "id": item_id,
                    "title": f"[{doc_type}] {title}" if doc_type else title,
                    "description": json.dumps(item)[:1000], "source": source,
                })

            if result["items"]:
                result["success"] = True
                log.info(f"LCD What's New: {len(result['items'])} items via {path}")
                break

        except Exception as e:
            log.debug(f"LCD endpoint {path} error: {e}")
            continue

    result["items_found"] = len(result["items"])
    if not result["success"] and not result["items"]:
        result["error"] = "All LCD endpoints returned errors or empty data"
    return result


def scan_proposed_lcds() -> dict:
    """Scan proposed Local Coverage Determinations with token auth."""
    source = "Proposed LCDs"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        token = get_lcd_token()
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        for path in ["/v1/data/lcd/", "/v1/reports/local-coverage-proposed-lcds/"]:
            resp = CMSScanner.session.get(f"{CMS_API_BASE}{path}", timeout=15, headers=headers)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", data.get("results", []))
            for item in (items or [])[:50]:
                item_id = str(item.get("id", item.get("documentId", "")))
                title = item.get("title", item.get("documentTitle", ""))
                if not item_id and title:
                    item_id = hashlib.md5(title.encode()).hexdigest()[:12]
                if not item_id:
                    continue
                result["items"].append({
                    "id": item_id, "title": title,
                    "description": json.dumps(item)[:1000], "source": source,
                })
            if result["items"]:
                break

        result["items_found"] = len(result["items"])
        result["success"] = len(result["items"]) > 0
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Proposed LCD scan error: {e}")
    return result


def scan_final_lcds() -> dict:
    """Scan final Local Coverage Determinations with token auth."""
    source = "Final LCDs"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        token = get_lcd_token()
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        for path in ["/v1/data/lcd/", "/v1/reports/local-coverage-final-lcds/"]:
            resp = CMSScanner.session.get(f"{CMS_API_BASE}{path}", timeout=15, headers=headers)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", data.get("results", []))
            for item in (items or [])[:50]:
                item_id = str(item.get("id", item.get("documentId", "")))
                title = item.get("title", item.get("documentTitle", ""))
                if not item_id and title:
                    item_id = hashlib.md5(title.encode()).hexdigest()[:12]
                if not item_id:
                    continue
                result["items"].append({
                    "id": item_id, "title": title,
                    "description": json.dumps(item)[:1000], "source": source,
                })
            if result["items"]:
                break

        result["items_found"] = len(result["items"])
        result["success"] = len(result["items"]) > 0
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Final LCD scan error: {e}")
    return result


def scan_medcac_meetings() -> dict:
    """Scan MEDCAC advisory committee meetings."""
    source = "MEDCAC Meetings"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/national-coverage-medcac-meetings/")
        if resp is None:
            result["error"] = "API request failed"
            return result
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", data.get("results", []))
        for item in items[:20]:
            item_id = str(item.get("id", item.get("meetingId", "")))
            title = item.get("title", item.get("meetingTitle", ""))
            if not item_id and title:
                item_id = hashlib.md5(title.encode()).hexdigest()[:12]
            result["items"].append({
                "id": item_id, "title": title,
                "description": json.dumps(item)[:1000], "source": source,
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"MEDCAC scan error: {e}")
    return result


def scan_technology_assessments() -> dict:
    """Scan Technology Assessments supporting NCDs."""
    source = "Technology Assessments"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/national-coverage-technology-assessments/")
        if resp is None:
            result["error"] = "API request failed"
            return result
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", data.get("results", []))
        for item in items[:20]:
            item_id = str(item.get("id", item.get("taId", "")))
            title = item.get("title", item.get("taTitle", ""))
            if not item_id and title:
                item_id = hashlib.md5(title.encode()).hexdigest()[:12]
            result["items"].append({
                "id": item_id, "title": title,
                "description": json.dumps(item)[:1000], "source": source,
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Technology Assessment scan error: {e}")
    return result


def scan_cms_newsroom() -> dict:
    """Scrape CMS newsroom for press releases."""
    source = "CMS Newsroom"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get("https://www.cms.gov/about-cms/contact/newsroom", accept_json=False, timeout=20)
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.select("article, .views-row, .node--type-press-release, li.views-row")
        if not articles:
            articles = soup.select("div.view-content a, .field-content a")
        for art in articles[:30]:
            link = art if art.name == "a" else art.find("a")
            if not link:
                continue
            title = link.get_text(strip=True)
            href = link.get("href", "")
            if href and not href.startswith("http"):
                href = f"https://www.cms.gov{href}"
            if not title or len(title) < 10:
                continue
            item_id = hashlib.md5(href.encode()).hexdigest()[:12] if href else hashlib.md5(title.encode()).hexdigest()[:12]
            date_el = art.find(class_=lambda c: c and "date" in c.lower()) if art.name != "a" else None
            date_str = date_el.get_text(strip=True) if date_el else ""
            result["items"].append({
                "id": item_id, "title": title,
                "description": f"Date: {date_str}\nURL: {href}",
                "source": source, "url": href,
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"CMS Newsroom scan error: {e}")
    return result


def scan_hcpcs_updates() -> dict:
    """Scrape HCPCS quarterly update page for new code files."""
    source = "HCPCS Updates"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update",
            accept_json=False, timeout=20,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if not text or len(text) < 5:
                continue
            if any(kw in text.lower() for kw in ["hcpcs file", "alpha-numeric", "update", "quarterly"]) or \
               any(kw in href.lower() for kw in [".zip", "hcpcs", "quarterly"]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text,
                    "description": f"HCPCS Quarterly Update File: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"HCPCS update scan error: {e}")
    return result


def scan_hcpcs_coding_decisions() -> dict:
    """Scrape HCPCS coding decisions page for drug/biological code decisions."""
    source = "HCPCS Coding"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/current-prior-years-level-ii-coding-decisions",
            accept_json=False, timeout=20,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in ["application summary", "coding", "determination", "drugs", "biologicals", "public meeting"]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text,
                    "description": f"HCPCS Coding Decision/Application: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"HCPCS Coding Decisions scan error: {e}")
    return result


def scan_cms_coverage_main() -> dict:
    """Scrape CMS Medicare coverage determination page."""
    source = "CMS Coverage Page"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coverage/determination-process",
            accept_json=False, timeout=20,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in ["ncd", "coverage", "determination", "tracking", "decision memo", "proposed"]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text,
                    "description": f"CMS Coverage Update: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"CMS Coverage page scan error: {e}")
    return result


def scan_federal_register_cms() -> dict:
    """Scan Federal Register API for CMS-related proposed/final rules."""
    source = "Federal Register"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        today = datetime.now(EST).strftime("%Y-%m-%d")
        week_ago = (datetime.now(EST) - timedelta(days=7)).strftime("%Y-%m-%d")
        resp = CMSScanner.safe_get(
            "https://www.federalregister.gov/api/v1/documents.json",
            params={
                "conditions[agencies][]": "centers-for-medicare-medicaid-services",
                "conditions[publication_date][gte]": week_ago,
                "conditions[publication_date][lte]": today,
                "per_page": 50, "order": "newest",
            },
            timeout=20,
        )
        if resp is None:
            result["error"] = "Federal Register API failed"
            return result
        data = resp.json()
        for doc in data.get("results", []):
            doc_id = doc.get("document_number", "")
            title = doc.get("title", "")
            doc_type = doc.get("type", "")
            abstract = (doc.get("abstract") or "")[:500]
            pub_date = doc.get("publication_date", "")
            html_url = doc.get("html_url", "")
            result["items"].append({
                "id": doc_id,
                "title": f"[{doc_type}] {title}",
                "description": f"Published: {pub_date}\n{abstract}\nURL: {html_url}",
                "source": source, "url": html_url,
            })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Federal Register scan error: {e}")
    return result


# ─── TIER 1: Massive Stock Movers ────────────────────────────────────

def scan_drug_price_negotiation() -> dict:
    """Scan Medicare Drug Price Negotiation Program page."""
    source = "Drug Price Negotiation"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/priorities/medicare-prescription-drug-affordability/overview/medicare-drug-price-negotiation-program/selected-drugs-negotiated-prices",
            accept_json=False, timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "selected drug", "negotiated price", "maximum fair price",
                "fact sheet", "manufacturer agreement", "mfp explanation",
                "infographic", "drug list", "renegotiation",
            ]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"Drug Negotiation Program: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Drug negotiation scan error: {e}")
    return result


def scan_drug_negotiation_guidance() -> dict:
    """Scan Drug Price Negotiation guidance page."""
    source = "Drug Negotiation Guidance"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/priorities/medicare-prescription-drug-affordability/overview/medicare-drug-price-negotiation-program/guidance-policy-documents",
            accept_json=False, timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "guidance", "final", "draft", "revised", "price applicability",
                "initial offer", "maximum fair", "fact sheet", "negotiation",
                "comment", "icr", "biotech exception", "biosimilar delay",
            ]) or href.endswith(".pdf") or href.endswith(".zip"):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"Drug Negotiation Guidance: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Drug negotiation guidance scan error: {e}")
    return result


def scan_asp_pricing_files() -> dict:
    """Scan Part B ASP drug pricing files page."""
    source = "ASP Drug Pricing"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/payment/part-b-drugs/asp-pricing-files",
            accept_json=False, timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "asp pricing", "payment limit", "noc pricing", "crosswalk",
                "ndc-hcpcs", "final file", "revised",
            ]) or ".zip" in href.lower():
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"Part B ASP Drug Pricing File: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"ASP pricing scan error: {e}")
    return result


def scan_ma_rate_announcements() -> dict:
    """Scan Medicare Advantage rate announcements."""
    source = "MA Rate Announcements"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/health-plans/medicareadvtgspecratestats/announcements-and-documents",
            accept_json=False, timeout=10,
        )
        if resp is None:
            resp = CMSScanner.safe_get(
                "https://www.cms.gov/medicare/health-plans/medicareadvtgspecratestats",
                accept_json=False, timeout=10,
            )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "advance notice", "rate announcement", "announcement",
                "fact sheet", "call letter", "final notice",
                "star rating", "payment", "benchmark",
            ]) or href.endswith(".pdf"):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"MA Rate/Payment Update: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"MA rate announcements scan error: {e}")
    return result


def scan_star_ratings() -> dict:
    """Scan CMS Star Ratings page for MA quality ratings updates."""
    source = "MA Star Ratings"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data",
            accept_json=False, timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "star rating", "quality", "display measure",
                "performance", "technical notes", "data table",
                "fact sheet", "cut point", "categorical adjustment",
            ]) or href.endswith(".zip") or href.endswith(".pdf"):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"MA Star Ratings: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Star ratings scan error: {e}")
    return result


def scan_enforcement_actions() -> dict:
    """Scan CMS enforcement actions: sanctions, CMPs, terminations.

    TIER 1 source. This catches Elevance-type sanctions, civil money penalties
    against MA/Part D sponsors, pharma manufacturer penalties, and compliance actions.
    Impacts: UNH, ELV, HUM, CVS, CNC, MOH and pharma stocks.
    """
    source = "CMS Enforcement Actions"
    result = {"success": False, "items_found": 0, "items": []}

    enforcement_urls = [
        ("https://www.cms.gov/medicare/audits-compliance/part-c-d/part-c-and-part-d-enforcement-actions",
         "Part C/D Enforcement"),
        ("https://www.cms.gov/medicare/audits-compliance/part-c-d/pharmaceutical-manufacturer-enforcement-actions",
         "Pharma Manufacturer Enforcement"),
        ("https://www.cms.gov/medicare/audits-compliance/part-c-d/actions",
         "Compliance Actions"),
    ]

    any_success = False
    for url, subsource in enforcement_urls:
        try:
            resp = CMSScanner.safe_get(url, accept_json=False, timeout=20)
            if resp is None:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find main content area (skip nav/footer)
            main = soup.select_one(
                "main, #skipNavTarget, .field--name-body, "
                ".layout-content, article, #block-cms-evo-content"
            )
            search_area = main if main else soup

            for link in search_area.find_all("a", href=True):
                text = link.get_text(strip=True)
                href = link["href"]
                if not text or len(text) < 5:
                    continue

                # Skip nav/menu links
                if any(skip in href.lower() for skip in [
                    "/about-cms", "/newsroom", "/data-research",
                    "javascript:", "#", "mailto:", "/themes/",
                ]):
                    continue

                # Include enforcement-relevant links
                is_enforcement = (
                    href.lower().endswith(".pdf") or
                    any(kw in text.lower() for kw in [
                        "sanction", "cmp", "penalty", "termination",
                        "enforcement", "corrective", "warning",
                        "civil money", "intermediate", "suspension",
                    ]) or
                    any(kw in href.lower() for kw in [
                        "sanction", "cmp", "penalty", "termination",
                    ])
                )

                if not is_enforcement:
                    continue

                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"

                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id,
                    "title": f"[{subsource}] {text}",
                    "description": f"CMS Enforcement: {text}\nType: {subsource}\nURL: {href}",
                    "source": source, "url": href,
                })

            any_success = True
        except Exception as e:
            log.error(f"Enforcement scan error ({subsource}): {e}")

    result["items_found"] = len(result["items"])
    result["success"] = any_success
    if not any_success:
        result["error"] = "All enforcement pages failed"
    return result


# ─── TIER 2: Significant Movers ──────────────────────────────────────

def scan_cmmi_models() -> dict:
    """Scan CMS Innovation Center pages for new/modified payment models."""
    source = "CMMI Innovation Models"
    result = {"success": False, "items_found": 0, "items": []}
    pages = [
        "https://www.cms.gov/priorities/innovation",
        "https://www.cms.gov/priorities/innovation/data-and-reports",
    ]
    seen_hrefs = set()
    for page_url in pages:
        try:
            resp = CMSScanner.safe_get(page_url, accept_json=False, timeout=10)
            if resp is None:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True)
                href = link["href"]
                if not text or len(text) < 10:
                    continue
                if any(kw in href.lower() for kw in [
                    "/innovation/innovation-models/",
                    "/innovation/data-and-reports/",
                    "/innovation-insight",
                ]) or any(kw in text.lower() for kw in [
                    "model", "innovation", "payment", "demonstration",
                    "evaluation", "fact sheet", "request for application",
                ]):
                    if not href.startswith("http"):
                        href = f"https://www.cms.gov{href}"
                    if href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)
                    item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                    result["items"].append({
                        "id": item_id, "title": text[:300],
                        "description": f"CMMI: {href}",
                        "source": source, "url": href,
                    })
        except Exception as e:
            log.debug(f"CMMI page error ({page_url}): {e}")
            continue

    result["items_found"] = len(result["items"])
    result["success"] = len(result["items"]) > 0 or len(seen_hrefs) == 0
    if not result["items"] and not result["success"]:
        result["error"] = "No CMMI data retrieved"
    return result


def scan_ipps_opps() -> dict:
    """Scan CMS hospital payment (IPPS/OPPS) pages."""
    source = "Hospital Payments (IPPS/OPPS)"
    result = {"success": False, "items_found": 0, "items": []}
    pages = [
        "https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps",
        "https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient",
    ]
    for page_url in pages:
        try:
            resp = CMSScanner.safe_get(page_url, accept_json=False, timeout=10)
            if resp is None:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True)
                href = link["href"]
                if not text or len(text) < 10:
                    continue
                if any(kw in text.lower() for kw in [
                    "final rule", "proposed rule", "fact sheet",
                    "new technology", "ntap", "add-on payment",
                    "payment update", "ipps", "opps", "correction notice",
                ]) or href.endswith(".pdf"):
                    if not href.startswith("http"):
                        href = f"https://www.cms.gov{href}"
                    item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                    result["items"].append({
                        "id": item_id, "title": text[:300],
                        "description": f"Hospital Payment Update: {href}",
                        "source": source, "url": href,
                    })
        except Exception as e:
            log.debug(f"IPPS/OPPS page error: {e}")
            continue

    result["items_found"] = len(result["items"])
    result["success"] = len(result["items"]) > 0
    if not result["success"]:
        result["error"] = "No IPPS/OPPS data retrieved"
    return result


def scan_cms_transmittals() -> dict:
    """Scan CMS transmittals/change requests."""
    source = "CMS Transmittals"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/regulations-and-guidance/guidance/transmittals",
            accept_json=False, timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "transmittal", "change request", "cr ", "r1",
            ]) or any(kw in href.lower() for kw in [
                "/transmittals/", ".pdf",
            ]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"CMS Transmittal: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"CMS Transmittals scan error: {e}")
    return result


def scan_dmepos() -> dict:
    """Scan DMEPOS competitive bidding page."""
    source = "DMEPOS Bidding"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/payment/fee-schedules/dmepos-competitive-bidding",
            accept_json=False, timeout=10,
        )
        if resp is None:
            resp = CMSScanner.safe_get(
                "https://www.cms.gov/medicare/medicare-fee-for-service-payment/dmeposfeesched",
                accept_json=False, timeout=10,
            )
        if resp is None:
            result["error"] = "Page request failed"
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True)
            href = link["href"]
            if not text or len(text) < 10:
                continue
            if any(kw in text.lower() for kw in [
                "competitive bid", "single payment amount", "fee schedule",
                "round", "product category", "contract supplier",
                "dmepos", "fact sheet", "announcement",
            ]) or href.endswith(".zip") or href.endswith(".pdf"):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id, "title": text[:300],
                    "description": f"DMEPOS Update: {href}",
                    "source": source, "url": href,
                })
        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"DMEPOS scan error: {e}")
    return result


# ─── Alert Formatting ─────────────────────────────────────────────────

IMPACT_COLORS = {
    "HIGH": 0xFF0000,      # Red
    "MEDIUM": 0xFF8C00,    # Orange
    "LOW": 0xFFD700,       # Gold
    "NONE": 0x808080,      # Gray
}

CATEGORY_EMOJI = {
    "NEW_CODE": "🆕",
    "COVERAGE_CHANGE": "📋",
    "PRODUCT_APPROVAL": "✅",
    "PRICING": "💰",
    "POLICY": "📜",
    "MEETING": "🗓️",
    "DRUG_NEGOTIATION": "💊",
    "RATE_CHANGE": "📊",
    "MODEL_CHANGE": "🏗️",
    "PAYMENT_UPDATE": "💵",
    "ENFORCEMENT": "⚖️",
    "OTHER": "📌",
}

def format_alert_embed(item: dict, analysis: dict) -> dict:
    impact = analysis.get("impact_level", "NONE")
    category = analysis.get("category", "OTHER")
    tickers = analysis.get("tickers", [])
    companies = analysis.get("company_names", [])
    summary = analysis.get("summary", "")
    revenue_impact = analysis.get("revenue_impact", "")
    revenue_summary = analysis.get("revenue_impact_summary", "")

    emoji = CATEGORY_EMOJI.get(category, "📌")
    ticker_str = " ".join(f"`${t}`" for t in tickers) if tickers else "None identified"
    company_str = ", ".join(companies) if companies else "N/A"
    signal = analysis.get("signal", "NEUTRAL")

    # Signal emoji
    signal_display = {"BULLISH": "🟢 BULLISH ↑", "BEARISH": "🔴 BEARISH ↓", "NEUTRAL": "⚪ NEUTRAL"}.get(signal, "⚪ NEUTRAL")

    # Detection timestamp
    detected_at = datetime.now(EST).strftime("%m/%d/%Y %I:%M:%S %p EST")
    doc_date = item.get("_doc_date", "")

    fields = [
        {"name": "📊 Impact Level", "value": f"**{impact}**", "inline": True},
        {"name": "🎯 Signal", "value": f"**{signal_display}**", "inline": True},
        {"name": "💰 Rev Impact", "value": f"**{revenue_summary}**" if revenue_summary else "N/A", "inline": True},
        {"name": "🏢 Companies", "value": company_str, "inline": True},
        {"name": "📈 Tickers", "value": ticker_str, "inline": True},
        {"name": "📅 CMS Action Date", "value": doc_date if doc_date else "Unknown", "inline": True},
    ]

    if summary:
        fields.append({"name": "💡 AI Analysis", "value": summary[:1024], "inline": False})

    if revenue_impact:
        fields.append({"name": "💵 Revenue Impact Detail", "value": revenue_impact[:1024], "inline": False})

    url = item.get("url", "")

    # Build description — source URL prominently at top
    desc_parts = []
    if url:
        desc_parts.append(f"🔗 **[View Source Document on CMS.gov]({url})**")
    # Only include description text if it's not raw JSON
    raw_desc = item.get("description", "")
    if raw_desc and not raw_desc.lstrip().startswith("{") and not raw_desc.lstrip().startswith("["):
        # Clean up enriched content markers
        clean_desc = raw_desc.split("--- PAGE CONTENT ---")[0].strip()
        if clean_desc and len(clean_desc) > 10:
            desc_parts.append(clean_desc[:300])

    # Embed color based on signal + impact
    if signal == "BULLISH":
        color = 0x00CC66 if impact in ("HIGH", "MEDIUM") else 0x88CC88
    elif signal == "BEARISH":
        color = 0xFF0000 if impact in ("HIGH", "MEDIUM") else 0xFF8C00
    else:
        color = IMPACT_COLORS.get(impact, 0x808080)

    embed = {
        "title": f"{emoji} {item.get('title', 'CMS Update')[:250]}",
        "description": "\n\n".join(desc_parts) if desc_parts else "",
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": f"CMS Monitor • {item.get('source', '')} • Detected {detected_at}"},
    }
    if url:
        embed["url"] = url
    return embed


# ─── Content Enrichment ──────────────────────────────────────────────

def enrich_item_content(item: dict) -> str:
    """Fetch actual page/document content for better AI analysis."""
    url = item.get("url", "")
    base_desc = item.get("description", "")

    if not url:
        return base_desc

    try:
        # Skip binary files
        if any(url.lower().endswith(ext) for ext in [".zip", ".xlsx", ".xls", ".csv"]):
            return base_desc

        resp = CMSScanner.safe_get(url, accept_json=False, timeout=8)
        if resp is None:
            return base_desc

        content_type = resp.headers.get("Content-Type", "")

        # For PDFs, just note it's a PDF
        if "pdf" in content_type or url.lower().endswith(".pdf"):
            return f"{base_desc}\n[PDF document - title and URL only]"

        # For HTML pages, extract meaningful text
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav, footer, scripts, styles
        for tag in soup.find_all(["nav", "footer", "script", "style", "header", "aside"]):
            tag.decompose()

        # Try to find main content area
        main = soup.find("main") or soup.find(attrs={"role": "main"}) or soup.find(id="main-content")
        if main:
            text = main.get_text(separator="\n", strip=True)
        else:
            body = soup.find("body")
            text = body.get_text(separator="\n", strip=True) if body else ""

        # Clean up
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        text = "\n".join(lines)

        # Truncate to fit in DeepSeek context
        if len(text) > 3000:
            text = text[:3000] + "\n... [truncated]"

        if text:
            return f"{base_desc}\n\n--- PAGE CONTENT ---\n{text}"

    except Exception as e:
        log.debug(f"Content enrichment failed for {url}: {e}")

    return base_desc


# ─── DeepSeek AI Analysis ────────────────────────────────────────────

def analyze_with_deepseek(title: str, description: str, source: str) -> dict | None:
    """Use DeepSeek to analyze a CMS update for stock-moving potential."""
    prompt = f"""You are a financial analyst specializing in healthcare stocks and CMS (Centers for Medicare & Medicaid Services) policy.

Analyze this CMS update and determine:
1. What SPECIFICALLY changed? (exact codes, drug names, procedure names, payment rates, coverage decisions)
2. Which SPECIFIC publicly traded companies are directly affected and WHY?
3. What is the stock impact? Only mark HIGH/MEDIUM if there's a clear, specific financial impact on an identifiable company.
4. REVENUE IMPACT: Estimate how meaningful this change could be to each affected company's revenue. Consider:
   - What % of the company's total revenue could this product/service/code represent?
   - Is this a new revenue stream, expansion of existing, or a restriction/loss?
   - What is the approximate addressable market size for this specific change?
   - How quickly could this impact financials (immediate vs. gradual rollout)?
   Give a concrete dollar estimate range if possible (e.g. "$50M-$200M annual revenue at risk")
   or a percentage of revenue. If you cannot estimate, explain why.

IMPORTANT RULES:
- Do NOT list generic medtech/pharma companies unless the content specifically names their products or codes related to them
- If the content is too vague to identify specific companies, set involves_public_company to false and impact_level to NONE
- The summary MUST include the specific codes, drug names, or payment changes - never be generic
- Only include tickers for companies with a DIRECT, identifiable connection to the specific change
- revenue_impact MUST contain a quantified estimate or explain why one cannot be made
- signal MUST be BULLISH, BEARISH, or NEUTRAL for the affected stock(s). New/expanded coverage = BULLISH, coverage restriction/removal = BEARISH, sanctions/penalties = BEARISH, rate increases = BULLISH, rate cuts = BEARISH, new approvals = BULLISH

CMS Update:
Source: {source}
Title: {title}
Details: {description[:4000]}

Respond in this exact JSON format only, no other text:
{{
    "involves_public_company": true/false,
    "tickers": ["TICKER1", "TICKER2"],
    "impact_level": "HIGH/MEDIUM/LOW/NONE",
    "signal": "BULLISH/BEARISH/NEUTRAL",
    "company_names": ["Company Name 1"],
    "summary": "SPECIFIC summary: what code/drug/rate changed, how it affects the named company. Never be generic.",
    "revenue_impact_summary": "Short 1-line summary for headline display, e.g. '$50-200M rev at risk (~3% of rev)' or '<1% of rev, ~$0-50M' or '$2B+ new market (~15% rev upside)'. Always include dollar range AND percentage.",
    "revenue_impact": "Estimated revenue impact: $X-$Y annual revenue affected, representing ~Z% of company total revenue. Explain the math briefly.",
    "category": "NEW_CODE/COVERAGE_CHANGE/PRODUCT_APPROVAL/PRICING/POLICY/MEETING/DRUG_NEGOTIATION/RATE_CHANGE/MODEL_CHANGE/PAYMENT_UPDATE/ENFORCEMENT/OTHER"
}}"""

    try:
        resp = requests.post(
            DEEPSEEK_API_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 800,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()

        # Parse JSON from response (handle markdown code blocks)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        return json.loads(content)
    except Exception as e:
        log.error(f"DeepSeek analysis failed: {e}")
        return None


# ─── Main Scan Orchestrator ──────────────────────────────────────────

ALL_SCANNERS = {
    # Original 12 sources
    "NCD Reports": scan_ncd_reports,
    "NCD Coverage Docs": scan_ncd_whats_new,
    "LCD What's New": scan_lcd_whats_new,
    "Proposed LCDs": scan_proposed_lcds,
    "Final LCDs": scan_final_lcds,
    "MEDCAC Meetings": scan_medcac_meetings,
    "Tech Assessments": scan_technology_assessments,
    "CMS Newsroom": scan_cms_newsroom,
    "HCPCS Updates": scan_hcpcs_updates,
    "HCPCS Coding": scan_hcpcs_coding_decisions,
    "Coverage Page": scan_cms_coverage_main,
    "Federal Register": scan_federal_register_cms,
    # Tier 1: Massive stock movers
    "Drug Price Negotiation": scan_drug_price_negotiation,
    "Drug Negotiation Guidance": scan_drug_negotiation_guidance,
    "ASP Drug Pricing": scan_asp_pricing_files,
    "MA Rate Announcements": scan_ma_rate_announcements,
    "MA Star Ratings": scan_star_ratings,
    "CMS Enforcement Actions": scan_enforcement_actions,
    # Tier 2: Significant movers
    "CMMI Innovation Models": scan_cmmi_models,
    "Hospital Payments (IPPS/OPPS)": scan_ipps_opps,
    "CMS Transmittals": scan_cms_transmittals,
    "DMEPOS Bidding": scan_dmepos,
}

# High-value sources that get alerts even when DeepSeek is unavailable
HIGH_VALUE_SOURCES = {
    "NCD Reports", "NCD Coverage Docs", "Federal Register", "CMS Newsroom",
    "Drug Price Negotiation", "Drug Negotiation Guidance", "ASP Drug Pricing",
    "MA Rate Announcements", "MA Star Ratings", "CMS Enforcement Actions",
}


# ─── Item Recency Check ──────────────────────────────────────────────

def build_cms_url(item: dict) -> str:
    """Build a CMS Coverage Database URL for items that are missing one."""
    source = item.get("source", "")
    item_id = item.get("id", "")

    # Try to extract fields from description JSON
    meta = {}
    desc = item.get("description", "")
    try:
        if desc.lstrip().startswith("{"):
            meta = json.loads(desc)
    except (json.JSONDecodeError, ValueError):
        pass

    # Check if the API response itself contains a url/href
    for url_field in ("url", "html_url", "href", "link"):
        val = meta.get(url_field, "")
        if val and isinstance(val, str):
            # Skip CMS Coverage Database API internal paths — these aren't real web pages
            if val.startswith(("/data/", "/v1/")):
                continue
            if val.startswith("http"):
                return val
            if val.startswith("/"):
                return f"https://www.cms.gov{val}"

    # Build URL based on source type
    if "NCD" in source:
        ncd_id = meta.get("ncd_id", meta.get("ncdId", meta.get("ncdid", "")))
        ncd_ver = meta.get("ncd_version", meta.get("ncdVersion", meta.get("ncdver", "1")))
        if ncd_id and str(ncd_id).isdigit():
            return f"https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?NCDId={ncd_id}&ncdver={ncd_ver}"
        # For NCD Reports scanner where item_id IS the ncd_id
        if source == "NCD Reports" and item_id.isdigit():
            return f"https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?NCDId={item_id}"
        # For coverage docs, try document_display_id or fall through
        display_id = meta.get("document_display_id", meta.get("documentDisplayId", ""))
        if display_id:
            return f"https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?NCDId={display_id}"
        # Last resort: search by title
        title = item.get("title", "")
        if title:
            return f"https://www.cms.gov/medicare-coverage-database/search?q={quote(title[:80])}"

    if "LCD" in source:
        lcd_id = meta.get("id", meta.get("documentId", meta.get("lcdId", item_id)))
        if lcd_id and str(lcd_id).isdigit():
            return f"https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid={lcd_id}"
        # Articles
        article_id = meta.get("articleId", meta.get("article_id", ""))
        if article_id and str(article_id).isdigit():
            return f"https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleid={article_id}"

    if "MEDCAC" in source:
        return "https://www.cms.gov/medicare-coverage-database/view/medcac-meetings.aspx"

    if "Technology" in source:
        ta_id = meta.get("id", meta.get("taId", item_id))
        if ta_id and str(ta_id).isdigit():
            return f"https://www.cms.gov/medicare-coverage-database/view/technology-assessments.aspx?TAId={ta_id}"

    # Fallback: link to CMS coverage database search
    return "https://www.cms.gov/medicare-coverage-database/search"

# Common date formats found in CMS API responses
_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",      # 2024-11-27T00:00:00
    "%Y-%m-%dT%H:%M:%S.%f",   # 2024-11-27T00:00:00.000
    "%Y-%m-%d",                # 2024-11-27
    "%m/%d/%Y",                # 11/27/2024
    "%m/%d/%y",                # 11/27/24
    "%B %d, %Y",               # November 27, 2024
    "%b %d, %Y",               # Nov 27, 2024
]

# Fields in CMS API JSON that typically contain the document's update/publish date
_DATE_FIELDS = [
    "last_updated", "lastUpdated", "last_updated_sort",
    "publication_date", "publicationDate", "pub_date",
    "effective_date", "effectiveDate",
    "date", "updated", "posted_date", "postedDate",
    "document_date", "release_date", "releaseDate",
    "modified", "lastModified", "last_modified",
]


def extract_item_date(item: dict) -> datetime | None:
    """Try to extract the document's actual date from its description JSON or fields.

    Returns a timezone-aware datetime in EST, or None if no date found.
    """
    # First, try to parse date fields from the description (which is often json.dumps of the API item)
    desc = item.get("description", "")
    candidates = []

    # Try parsing the description as JSON to get structured date fields
    try:
        if desc.lstrip().startswith("{") or desc.lstrip().startswith("["):
            parsed = json.loads(desc)
            if isinstance(parsed, dict):
                for field in _DATE_FIELDS:
                    val = parsed.get(field)
                    if val and isinstance(val, str):
                        candidates.append(val.strip())
    except (json.JSONDecodeError, ValueError):
        pass

    # Also check top-level item fields
    for field in _DATE_FIELDS:
        val = item.get(field)
        if val and isinstance(val, str):
            candidates.append(val.strip())

    # Try to parse each candidate
    for raw in candidates:
        # Handle sort-format dates like "20200417101916" or "20241127"
        if raw.isdigit():
            if len(raw) >= 14:
                try:
                    dt = datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
                    return dt.replace(tzinfo=EST)
                except ValueError:
                    pass
            if len(raw) >= 8:
                try:
                    dt = datetime.strptime(raw[:8], "%Y%m%d")
                    return dt.replace(tzinfo=EST)
                except ValueError:
                    pass
            continue

        # Strip trailing Z or timezone info for parsing
        clean = raw.replace("Z", "").split("+")[0].strip()
        for fmt in _DATE_FORMATS:
            try:
                dt = datetime.strptime(clean, fmt)
                return dt.replace(tzinfo=EST)
            except ValueError:
                continue

    return None


def is_item_recent(item: dict, max_age_days: int = MAX_ITEM_AGE_DAYS) -> bool:
    """Check if an item's document date is within the acceptable recency window.

    Returns True if:
    - The item date is within max_age_days, OR
    - No date could be extracted (benefit of the doubt)
    """
    item_date = extract_item_date(item)
    if item_date is None:
        return True  # No date found — allow it through, don't suppress
    cutoff = datetime.now(EST) - timedelta(days=max_age_days)
    return item_date >= cutoff


# ─── Main Scan Cycle ─────────────────────────────────────────────────

def run_scan_cycle(seen: dict) -> dict:
    """Run a complete scan cycle across all sources."""
    log.info("Starting scan cycle...")
    source_results = {}
    new_items = []
    any_success = False

    for name, scanner_fn in ALL_SCANNERS.items():
        try:
            result = scanner_fn()
            source_results[name] = result

            if result["success"]:
                any_success = True
                for item in result.get("items", []):
                    h = item_hash(item["source"], item["id"])
                    if h not in seen:
                        new_items.append(item)
                        seen[h] = {
                            "first_seen": datetime.now(EST).isoformat(),
                            "source": item["source"],
                            "title": item.get("title", "")[:100],
                        }
            else:
                log.warning(f"Scanner {name} failed: {result.get('error', 'unknown')}")

            # Small delay between scanners to be polite
            time.sleep(0.5)

        except Exception as e:
            log.error(f"Scanner {name} exception: {e}")
            source_results[name] = {"success": False, "items_found": 0, "error": str(e)}

    # Process new items through AI analysis
    alerts_sent = 0
    stale_skipped = 0
    for item in new_items:
        if shutdown_event.is_set():
            break

        # Skip stale items — still tracked in seen_items but don't waste AI calls or alert
        if not is_item_recent(item):
            item_date = extract_item_date(item)
            date_str = item_date.strftime("%Y-%m-%d") if item_date else "unknown"
            log.info(f"Skipping stale item (dated {date_str}): {item.get('title', '')[:80]}")
            stale_skipped += 1
            continue

        # Ensure every item has a URL
        if not item.get("url"):
            item["url"] = build_cms_url(item)

        # Extract and attach document date for embed display
        item_date = extract_item_date(item)
        if item_date:
            item["_doc_date"] = item_date.strftime("%m/%d/%Y")

        # Fetch actual page content for better AI analysis
        enriched_desc = enrich_item_content(item)

        analysis = analyze_with_deepseek(
            item.get("title", ""),
            enriched_desc,
            item.get("source", ""),
        )

        if analysis is None:
            # If AI fails, still alert on items from high-value sources
            if item["source"] in HIGH_VALUE_SOURCES:
                analysis = {
                    "involves_public_company": False,
                    "tickers": [],
                    "impact_level": "LOW",
                    "signal": "NEUTRAL",
                    "company_names": [],
                    "summary": "AI analysis unavailable - alerting due to high-value source",
                    "revenue_impact_summary": "",
                    "revenue_impact": "Unable to estimate - AI analysis failed",
                    "category": "OTHER",
                }
            else:
                continue

        # Alert on anything that involves a public company OR is HIGH/MEDIUM impact
        should_alert = (
            analysis.get("involves_public_company", False) or
            analysis.get("impact_level") in ("HIGH", "MEDIUM") or
            len(analysis.get("tickers", [])) > 0
        )

        if should_alert:
            embed = format_alert_embed(item, analysis)
            send_discord_alert(None, ALERTS_WEBHOOK, embed=embed)
            alerts_sent += 1
            time.sleep(1)  # Rate limit protection

    log.info(f"Scan cycle complete: {len(new_items)} new items, {stale_skipped} stale skipped, {alerts_sent} alerts sent")
    health.record_scan(any_success, source_results)
    save_seen(seen)
    return source_results


# ─── Health Check Thread ──────────────────────────────────────────────

def health_check_loop():
    log.info("Health check thread started")
    last_daily_report = None

    while not shutdown_event.is_set():
        try:
            now = datetime.now(EST)

            # Send daily health OK at 7 AM EST on market days
            if is_market_day() and now.hour == 7 and last_daily_report != now.date():
                send_health_ok()
                last_daily_report = now.date()
                health.status["last_health_report"] = now.isoformat()
                health.save()

            # Check for issues
            should_alert, reasons = health.should_alert_health()
            if should_alert:
                send_health_alert(reasons)

        except Exception as e:
            log.error(f"Health check error: {e}")
            try:
                send_health_alert(f"🔥 Health check itself errored: {str(e)[:200]}")
            except Exception:
                pass

        # Check every 10 minutes
        shutdown_event.wait(600)

    log.info("Health check thread stopped")


# ─── Main Loop ────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("CMS Coverage & Procedure Code Monitor Starting")
    log.info(f"Scan interval: {SCAN_INTERVAL_SECONDS}s ({SCAN_INTERVAL_SECONDS/60:.0f} min)")
    log.info(f"Active hours: {MARKET_OPEN_HOUR}:00 - {MARKET_CLOSE_HOUR}:00 EST")
    log.info(f"Data directory: {DATA_DIR}")
    log.info(f"Scanners: {len(ALL_SCANNERS)}")
    log.info("=" * 60)

    seen = load_seen()
    log.info(f"Loaded {len(seen)} previously seen items")

    # Start health check thread
    health_thread = threading.Thread(target=health_check_loop, daemon=True)
    health_thread.start()

    # Send startup notification
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "🚀 CMS Monitor Started",
        "description": (
            f"**Scan interval:** {SCAN_INTERVAL_SECONDS}s\n"
            f"**Active hours:** {MARKET_OPEN_HOUR}:00 - {MARKET_CLOSE_HOUR}:00 EST\n"
            f"**Sources:** {len(ALL_SCANNERS)}\n"
            f"**Known items:** {len(seen)}\n\n"
            "**Monitoring:**\n"
            "• National Coverage Determinations (NCDs)\n"
            "• Local Coverage Determinations (LCDs) [token auth]\n"
            "• HCPCS Procedure Code Updates\n"
            "• MEDCAC Advisory Meetings\n"
            "• Technology Assessments\n"
            "• CMS Newsroom & Press Releases\n"
            "• Federal Register CMS Rules\n"
            "• Drug Price Negotiation & Guidance\n"
            "• ASP Drug Pricing Files\n"
            "• MA Rate Announcements & Star Ratings\n"
            "• CMS Enforcement Actions (Sanctions, CMPs, Terminations)\n"
            "• CMMI Innovation Models\n"
            "• Hospital Payments (IPPS/OPPS)\n"
            "• CMS Transmittals & DMEPOS"
        ),
        "color": 0x00CC66,
        "timestamp": datetime.now(EST).isoformat(),
    })

    # Initial scan to populate baseline (even outside market hours)
    if not seen:
        log.info("First run - performing baseline scan to learn existing items...")
        run_scan_cycle(seen)
        log.info(f"Baseline established with {len(seen)} items")

    while not shutdown_event.is_set():
        try:
            if is_market_hours():
                run_scan_cycle(seen)
                shutdown_event.wait(SCAN_INTERVAL_SECONDS)
            else:
                now = datetime.now(EST)
                if is_market_day():
                    if now.hour < MARKET_OPEN_HOUR:
                        target = now.replace(hour=MARKET_OPEN_HOUR, minute=0, second=0)
                        wait_secs = (target - now).total_seconds()
                        log.info(f"Pre-market: sleeping {wait_secs/60:.0f} min until {MARKET_OPEN_HOUR}:00 EST")
                        shutdown_event.wait(min(wait_secs, 300))
                    else:
                        log.info("After market hours, sleeping 30 min...")
                        shutdown_event.wait(1800)
                else:
                    log.info("Non-market day, sleeping 1 hour...")
                    shutdown_event.wait(3600)

        except Exception as e:
            log.error(f"Main loop error: {e}\n{traceback.format_exc()}")
            try:
                send_health_alert(f"🔥 Main loop error: {str(e)[:300]}")
            except Exception:
                pass
            shutdown_event.wait(60)

    log.info("CMS Monitor shutting down gracefully")
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "🛑 CMS Monitor Stopped",
        "description": f"Shutdown at {datetime.now(EST).isoformat()}",
        "color": 0xFF4444,
    })


if __name__ == "__main__":
    main()
