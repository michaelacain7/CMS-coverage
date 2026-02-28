#!/usr/bin/env python3
"""
CMS Coverage & Procedure Code Monitor
Monitors CMS for new procedure codes, coverage determinations, product approvals,
and policy changes that could impact publicly traded companies.

Data Sources:
1. CMS Coverage API - NCDs, LCDs, MEDCAC meetings, technology assessments
2. CMS.gov web pages - HCPCS quarterly updates, coding decisions, press releases
3. CMS Newsroom - Press releases and announcements
4. Federal Register - CMS-related proposed and final rules

Uses DeepSeek AI to analyze each finding for public company relevance and stock impact.
Active 6 AM - 6 PM EST on market open days only.
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
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ─── Configuration ────────────────────────────────────────────────────────────

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
MARKET_OPEN_HOUR = 6   # 6 AM EST
MARKET_CLOSE_HOUR = int(os.getenv("CMS_MARKET_CLOSE", "21"))  # 9 PM EST (set to 18 for production)
SCAN_INTERVAL_SECONDS = int(os.getenv("CMS_SCAN_INTERVAL", "10"))  # Check every 10 seconds
HEALTH_CHECK_INTERVAL_HOURS = 6  # Health check every 6 hours during active period

# Persistence
DATA_DIR = Path(os.getenv("CMS_DATA_DIR", "/home/claude/cms_monitor/data"))
SEEN_FILE = DATA_DIR / "seen_items.json"
HEALTH_FILE = DATA_DIR / "health_status.json"
LOG_FILE = DATA_DIR / "cms_monitor.log"

# CMS Coverage API Base
CMS_API_BASE = "https://api.coverage.cms.gov"

# ─── Logging ──────────────────────────────────────────────────────────────────

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

# ─── Graceful Shutdown ───────────────────────────────────────────────────────

shutdown_event = threading.Event()

def handle_signal(signum, frame):
    log.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ─── Persistence ─────────────────────────────────────────────────────────────

def load_seen() -> dict:
    """Load previously seen items from disk."""
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text())
        except Exception:
            log.warning("Corrupt seen file, starting fresh")
    return {}

def save_seen(seen: dict):
    """Save seen items to disk."""
    SEEN_FILE.write_text(json.dumps(seen, indent=2, default=str))

def item_hash(source: str, item_id: str) -> str:
    """Create a unique hash for a source+item combination."""
    return hashlib.md5(f"{source}:{item_id}".encode()).hexdigest()

# ─── Health Tracking ─────────────────────────────────────────────────────────

class HealthTracker:
    """Tracks health metrics for the monitoring system."""

    def __init__(self):
        self.status = self._load()

    def _load(self) -> dict:
        if HEALTH_FILE.exists():
            try:
                return json.loads(HEALTH_FILE.read_text())
            except Exception:
                pass
        return {
            "last_scan_time": None,
            "last_scan_success": None,
            "total_scans": 0,
            "total_errors": 0,
            "total_alerts_sent": 0,
            "source_status": {},
            "consecutive_failures": 0,
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
        # Update per-source status
        for source, result in source_results.items():
            self.status["source_status"][source] = {
                "last_check": datetime.now(EST).isoformat(),
                "success": result.get("success", False),
                "items_found": result.get("items_found", 0),
                "error": result.get("error"),
            }
        self.save()

    def record_alert(self):
        self.status["total_alerts_sent"] += 1
        self.save()

    def should_alert_health(self) -> tuple[bool, str]:
        """Determine if a health alert should be sent."""
        reasons = []

        # Alert on 3+ consecutive failures
        if self.status["consecutive_failures"] >= 3:
            reasons.append(f"⚠️ {self.status['consecutive_failures']} consecutive scan failures")

        # Alert if any source has been failing
        for source, info in self.status.get("source_status", {}).items():
            if info.get("error") and not info.get("success"):
                reasons.append(f"❌ Source `{source}` failing: {info['error'][:100]}")

        # Alert if no scan in over 2 minutes during market hours
        if self.status["last_scan_time"]:
            last_scan = datetime.fromisoformat(self.status["last_scan_time"])
            now = datetime.now(EST)
            if (now - last_scan).total_seconds() > 120 and is_market_hours():
                reasons.append(f"⏰ No scan in {int((now - last_scan).total_seconds())}s")

        return (len(reasons) > 0, "\n".join(reasons))

health = HealthTracker()

# ─── Market Hours ────────────────────────────────────────────────────────────

# US Market Holidays 2025-2026 (NYSE/NASDAQ closed)
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
    """Check if today is a trading day (weekday and not a holiday)."""
    now = datetime.now(EST)
    date_str = now.strftime("%Y-%m-%d")
    return now.weekday() < 5 and date_str not in MARKET_HOLIDAYS

def is_market_hours() -> bool:
    """Check if current time is within 6 AM - 6 PM EST on a market day."""
    if not is_market_day():
        return False
    now = datetime.now(EST)
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR

# ─── Discord Webhook ─────────────────────────────────────────────────────────

def send_discord_alert(content: str, webhook_url: str = ALERTS_WEBHOOK, embed: dict = None):
    """Send a message to Discord via webhook."""
    payload = {}
    if content:
        # Truncate content to Discord limit
        if len(content) > 2000:
            content = content[:1997] + "..."
        payload["content"] = content
    if embed:
        # Truncate embed description
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
    """Send health status alert."""
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "🏥 CMS Monitor Health Alert",
        "description": message,
        "color": 0xFF4444,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "CMS Coverage Monitor • Health Check"},
    })

def send_health_ok():
    """Send daily health OK status."""
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

# ─── DeepSeek AI Analysis ───────────────────────────────────────────────────

def analyze_with_deepseek(title: str, description: str, source: str) -> dict | None:
    """Use DeepSeek to analyze a CMS update for stock-moving potential."""
    prompt = f"""You are a financial analyst specializing in healthcare stocks and CMS (Centers for Medicare & Medicaid Services) policy.

Analyze this CMS update and determine:
1. Does this involve or impact any publicly traded company? (If so, which ticker symbols?)
2. What is the potential stock impact? (HIGH / MEDIUM / LOW / NONE)
3. Brief explanation of why this matters for the market.

CMS Update:
Source: {source}
Title: {title}
Details: {description[:2000]}

Respond in this exact JSON format only, no other text:
{{
    "involves_public_company": true/false,
    "tickers": ["TICKER1", "TICKER2"],
    "impact_level": "HIGH/MEDIUM/LOW/NONE",
    "company_names": ["Company Name 1"],
    "summary": "Brief 1-2 sentence explanation of market impact",
    "category": "NEW_CODE/COVERAGE_CHANGE/PRODUCT_APPROVAL/PRICING/POLICY/MEETING/OTHER"
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
                "max_tokens": 500,
            },
            timeout=20,        )
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

# ─── CMS Data Source Scanners ────────────────────────────────────────────────

class CMSScanner:
    """Base scanner with common HTTP helpers."""

    session = requests.Session()
    session.headers.update({
        "User-Agent": "CMSCoverageMonitor/1.0 (Financial Research)",
        "Accept": "application/json",
    })

    @staticmethod
    def safe_get(url: str, params: dict = None, timeout: int = 10, accept_json: bool = True) -> requests.Response | None:
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

        for item in items[:50]:  # Limit processing
            item_id = str(item.get("ncd_id", item.get("id", item.get("documentId", ""))))
            title = item.get("title", item.get("ncdTitle", ""))
            if not item_id and title:
                item_id = hashlib.md5(title.encode()).hexdigest()[:12]
            result["items"].append({
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
                "url": f"https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid={item_id}" if item_id.isdigit() else "",
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"NCD scan error: {e}")
    return result


def scan_ncd_whats_new() -> dict:
    """Scan the NCD What's New report for recent changes."""
    source = "NCD What's New"
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
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"NCD What's New scan error: {e}")
    return result


def scan_lcd_whats_new() -> dict:
    """Scan Local Coverage reports for recent changes (articles + LCDs)."""
    source = "LCD What's New"
    result = {"success": False, "items_found": 0, "items": []}

    # The whats-new endpoint requires date params — try multiple endpoints silently
    lcd_endpoints = [
        "/v1/reports/local-coverage-whats-new",
        "/v1/reports/local-coverage-articles",
        "/v1/reports/local-coverage-articles/",
    ]

    for path in lcd_endpoints:
        try:
            resp = CMSScanner.session.get(
                f"{CMS_API_BASE}{path}", timeout=8,
                headers={"Accept": "application/json"}
            )
            if resp.status_code != 200:
                continue  # Silently try next endpoint

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
                    "description": json.dumps(item)[:1000],
                    "source": source,
                })

            if result["items"]:
                result["success"] = True
                break

        except Exception as e:
            continue

    result["items_found"] = len(result["items"])
    if not result["success"] and not result["items"]:
        result["error"] = "All LCD endpoints returned errors or empty data"
    return result


def scan_proposed_lcds() -> dict:
    """Scan proposed Local Coverage Determinations."""
    source = "Proposed LCDs"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/local-coverage-proposed-lcds/")
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
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Proposed LCD scan error: {e}")
    return result


def scan_final_lcds() -> dict:
    """Scan final Local Coverage Determinations."""
    source = "Final LCDs"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(f"{CMS_API_BASE}/v1/reports/local-coverage-final-lcds/")
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
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
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
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
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
                "id": item_id,
                "title": title,
                "description": json.dumps(item)[:1000],
                "source": source,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Technology Assessment scan error: {e}")
    return result


def scan_cms_newsroom() -> dict:
    """Scrape CMS newsroom for press releases and announcements."""
    source = "CMS Newsroom"
    result = {"success": False, "items_found": 0, "items": []}

    # CMS reorganized their site — try multiple known URLs
    newsroom_urls = [
        "https://www.cms.gov/about-cms/contact/newsroom",
        "https://www.cms.gov/newsroom",
    ]

    page_fetched = False
    soup = None
    for url in newsroom_urls:
        try:
            resp = CMSScanner.safe_get(url, accept_json=False, timeout=10)
            if resp and resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                page_fetched = True
                break
        except Exception:
            continue

    if not page_fetched or soup is None:
        result["error"] = "All newsroom URLs returned errors"
        return result

    try:
        # Find press release links — look for links containing newsroom paths
        seen_hrefs = set()
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if not text or len(text) < 15:
                continue

            # Match press release and fact sheet links
            is_newsroom_link = any(kw in href for kw in [
                "/newsroom/press-releases/",
                "/newsroom/fact-sheets/",
                "/newsroom/news-alert/",
            ])
            if not is_newsroom_link:
                continue

            if not href.startswith("http"):
                href = f"https://www.cms.gov{href}"

            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            item_id = hashlib.md5(href.encode()).hexdigest()[:12]

            # Get date from nearby elements
            parent = link.find_parent(["article", "li", "div"])
            date_el = parent.find(string=lambda s: s and any(m in s for m in [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
            ])) if parent else None
            date_str = date_el.strip() if date_el else ""

            result["items"].append({
                "id": item_id,
                "title": text[:300],
                "description": f"Date: {date_str}\nURL: {href}",
                "source": source,
                "url": href,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"CMS Newsroom parse error: {e}")
    return result


def scan_hcpcs_updates() -> dict:
    """Scrape HCPCS quarterly update page for new code files."""
    source = "HCPCS Updates"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update",
            accept_json=False,
            timeout=10,
        )
        if resp is None:
            result["error"] = "Page request failed"
            return result

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find all links to ZIP files and update announcements
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if not text or len(text) < 5:
                continue

            # Look for quarterly update files and announcements
            if any(kw in text.lower() for kw in ["hcpcs file", "alpha-numeric", "update", "quarterly"]) or \
               any(kw in href.lower() for kw in [".zip", "hcpcs", "quarterly"]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id,
                    "title": text,
                    "description": f"HCPCS Quarterly Update File: {href}",
                    "source": source,
                    "url": href,
                })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"HCPCS update scan error: {e}")
    return result


def scan_hcpcs_coding_decisions() -> dict:
    """Scrape HCPCS coding decisions page for new drug/biological code decisions."""
    source = "HCPCS Coding Decisions"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/current-prior-years-level-ii-coding-decisions",
            accept_json=False,
            timeout=10,
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
            # Look for application summaries, coding determinations
            if any(kw in text.lower() for kw in ["application summary", "coding", "determination", "drugs", "biologicals", "public meeting"]):
                if not href.startswith("http"):
                    href = f"https://www.cms.gov{href}"
                item_id = hashlib.md5(f"{text}:{href}".encode()).hexdigest()[:12]
                result["items"].append({
                    "id": item_id,
                    "title": text,
                    "description": f"HCPCS Coding Decision/Application: {href}",
                    "source": source,
                    "url": href,
                })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"HCPCS Coding Decisions scan error: {e}")
    return result


def scan_cms_coverage_main() -> dict:
    """Scrape the main CMS Medicare coverage page for announcements."""
    source = "CMS Coverage Page"
    result = {"success": False, "items_found": 0, "items": []}
    try:
        resp = CMSScanner.safe_get(
            "https://www.cms.gov/medicare/coverage/determination-process",
            accept_json=False,
            timeout=10,
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
                    "id": item_id,
                    "title": text,
                    "description": f"CMS Coverage Update: {href}",
                    "source": source,
                    "url": href,
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
                "per_page": 50,
                "order": "newest",
            },
            timeout=10,
        )
        if resp is None:
            result["error"] = "Federal Register API request failed"
            return result

        data = resp.json()
        if not isinstance(data, dict):
            result["error"] = f"Unexpected response type: {type(data)}"
            return result

        for doc in data.get("results", []):
            if not isinstance(doc, dict):
                continue
            doc_id = doc.get("document_number", "")
            title = doc.get("title", "")
            doc_type = doc.get("type", "")
            abstract = (doc.get("abstract") or "")[:500]
            pub_date = doc.get("publication_date", "")
            html_url = doc.get("html_url", "")

            if not doc_id:
                continue

            result["items"].append({
                "id": doc_id,
                "title": f"[{doc_type}] {title}" if doc_type else title,
                "description": f"Published: {pub_date}\n{abstract}\nURL: {html_url}",
                "source": source,
                "url": html_url,
            })

        result["items_found"] = len(result["items"])
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Federal Register scan error: {e}")
    return result


# ─── Alert Formatting ────────────────────────────────────────────────────────

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
    "OTHER": "📌",
}

def format_alert_embed(item: dict, analysis: dict) -> dict:
    """Format a Discord embed for a CMS alert."""
    impact = analysis.get("impact_level", "NONE")
    category = analysis.get("category", "OTHER")
    tickers = analysis.get("tickers", [])
    companies = analysis.get("company_names", [])
    summary = analysis.get("summary", "")

    emoji = CATEGORY_EMOJI.get(category, "📌")
    ticker_str = " ".join(f"`${t}`" for t in tickers) if tickers else "None identified"
    company_str = ", ".join(companies) if companies else "N/A"

    fields = [
        {"name": "📊 Impact Level", "value": f"**{impact}**", "inline": True},
        {"name": "🏷️ Category", "value": category.replace("_", " ").title(), "inline": True},
        {"name": "🏢 Companies", "value": company_str, "inline": True},
        {"name": "📈 Tickers", "value": ticker_str, "inline": True},
        {"name": "📝 Source", "value": item.get("source", "CMS"), "inline": True},
    ]

    if summary:
        fields.append({"name": "💡 AI Analysis", "value": summary[:1024], "inline": False})

    url = item.get("url", "")

    embed = {
        "title": f"{emoji} {item.get('title', 'CMS Update')[:250]}",
        "description": item.get("description", "")[:500],
        "color": IMPACT_COLORS.get(impact, 0x808080),
        "fields": fields,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": f"CMS Monitor • {item.get('source', '')}"},
    }
    if url:
        embed["url"] = url

    return embed

# ─── Main Scan Orchestrator ─────────────────────────────────────────────────

ALL_SCANNERS = {
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
}


def run_scan_cycle(seen: dict) -> dict:
    """Run a complete scan cycle across all sources."""
    log.info("Starting scan cycle...")
    source_results = {}
    new_items = []
    any_success = False

    for name, scanner_fn in ALL_SCANNERS.items():
        try:
            log.info(f"  Scanning: {name}...")
            result = scanner_fn()
            source_results[name] = result

            if result["success"]:
                any_success = True
                log.info(f"  ✅ {name}: {result['items_found']} items")
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
                log.warning(f"  ❌ {name}: {result.get('error', 'unknown')}")

            # Tiny delay between scanners (CMS allows 10k req/s)
            time.sleep(0.1)

        except Exception as e:
            log.error(f"Scanner {name} exception: {e}")
            source_results[name] = {"success": False, "items_found": 0, "error": str(e)}

    # Process new items through AI analysis
    alerts_sent = 0
    for item in new_items:
        if shutdown_event.is_set():
            break

        analysis = analyze_with_deepseek(
            item.get("title", ""),
            item.get("description", ""),
            item.get("source", ""),
        )

        if analysis is None:
            # If AI fails, still alert on items from high-value sources
            if item["source"] in ("NCD Reports", "NCD Coverage Docs", "Federal Register", "CMS Newsroom"):
                analysis = {
                    "involves_public_company": False,
                    "tickers": [],
                    "impact_level": "LOW",
                    "company_names": [],
                    "summary": "AI analysis unavailable - alerting due to high-value source",
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

    log.info(f"Scan cycle complete: {len(new_items)} new items, {alerts_sent} alerts sent")
    health.record_scan(any_success, source_results)
    save_seen(seen)
    return source_results


# ─── Health Check Thread ─────────────────────────────────────────────────────

def health_check_loop():
    """Separate thread that periodically checks system health."""
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


# ─── Main Loop ───────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("CMS Coverage & Procedure Code Monitor Starting")
    log.info(f"Scan interval: {SCAN_INTERVAL_SECONDS}s")
    log.info(f"Active hours: {MARKET_OPEN_HOUR}:00 - {MARKET_CLOSE_HOUR}:00 EST")
    log.info(f"Data directory: {DATA_DIR}")
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
            "• Local Coverage Determinations (LCDs)\n"
            "• HCPCS Procedure Code Updates\n"
            "• MEDCAC Advisory Meetings\n"
            "• Technology Assessments\n"
            "• CMS Newsroom & Press Releases\n"
            "• Federal Register CMS Rules\n"
            "• HCPCS Coding Decisions (Drugs & Biologicals)"
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
                # Wait for next scan interval
                shutdown_event.wait(SCAN_INTERVAL_SECONDS)
            else:
                now = datetime.now(EST)
                if is_market_day():
                    if now.hour < MARKET_OPEN_HOUR:
                        # Sleep until market open
                        target = now.replace(hour=MARKET_OPEN_HOUR, minute=0, second=0)
                        wait_secs = (target - now).total_seconds()
                        log.info(f"Pre-market: sleeping {wait_secs/60:.0f} min until {MARKET_OPEN_HOUR}:00 EST")
                        shutdown_event.wait(min(wait_secs, 300))
                    else:
                        # After market close, sleep until next check
                        log.info("After market hours, sleeping 30 min...")
                        shutdown_event.wait(1800)
                else:
                    # Weekend/holiday - sleep longer
                    log.info("Non-market day, sleeping 1 hour...")
                    shutdown_event.wait(3600)

        except Exception as e:
            log.error(f"Main loop error: {e}\n{traceback.format_exc()}")
            try:
                send_health_alert(f"🔥 Main loop error: {str(e)[:300]}")
            except Exception:
                pass
            shutdown_event.wait(60)  # Brief pause before retry

    log.info("CMS Monitor shutting down gracefully")
    send_discord_alert(None, HEALTH_WEBHOOK, embed={
        "title": "🛑 CMS Monitor Stopped",
        "description": f"Shutdown at {datetime.now(EST).isoformat()}",
        "color": 0xFF4444,
    })


if __name__ == "__main__":
    main()
