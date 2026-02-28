# CMS Coverage & Procedure Code Monitor

Real-time monitoring of CMS (Centers for Medicare & Medicaid Services) for new procedure codes, coverage determinations, product approvals, and policy changes that could impact publicly traded companies.

## What It Monitors (12 Sources)

### CMS Coverage API (No API key required)
| Source | What It Catches |
|--------|----------------|
| **NCD Reports** | National Coverage Determinations — national policy granting/limiting Medicare coverage |
| **NCD Coverage Docs** | Medicare coverage documents related to national-level coverage |
| **LCD What's New** | Recently added/updated Local Coverage Determinations and Articles |
| **Proposed LCDs** | Proposed Local Coverage Determinations before they become final |
| **Final LCDs** | Finalized Local Coverage Determinations |
| **MEDCAC Meetings** | Medicare Evidence Development & Coverage Advisory Committee meetings |
| **Technology Assessments** | TAs supporting the NCD process for medical technologies |

### Web Scrapers
| Source | What It Catches |
|--------|----------------|
| **CMS Newsroom** | Press releases and announcements from CMS |
| **HCPCS Updates** | Quarterly HCPCS procedure code update files (new/revised/deleted codes) |
| **HCPCS Coding Decisions** | Drug & biological coding decisions and application summaries |
| **Coverage Page** | NCD tracking sheets, decision memos, proposed determinations |
| **Federal Register** | CMS proposed rules, final rules, and notices (via Federal Register API) |

## How It Works

1. **Scans all 12 sources** every 5 minutes during market hours
2. **Detects new items** by comparing against a persistent hash database
3. **AI Analysis** — DeepSeek analyzes each new item for:
   - Public company involvement (with ticker symbols)
   - Stock impact level (HIGH/MEDIUM/LOW/NONE)
   - Category (NEW_CODE, COVERAGE_CHANGE, PRODUCT_APPROVAL, PRICING, POLICY, MEETING)
4. **Alerts** — Only sends Discord alerts for items involving public companies or HIGH/MEDIUM impact
5. **Health monitoring** — Separate thread checks system health, sends daily OK reports at 7 AM EST, and alerts on failures

## Schedule
- **Active:** 6:00 AM – 6:00 PM EST
- **Market days only** (Mon–Fri, excluding NYSE holidays)
- **Health check:** Daily at 7 AM EST + alerts on 3+ consecutive failures

## Discord Webhooks
- **Alerts webhook** → Stock-moving CMS updates with AI analysis
- **Health webhook** → System health, startup/shutdown, and error alerts

## Deploy to Railway

1. Push this folder to a GitHub repo
2. Create a new Railway project → "Deploy from GitHub"
3. Add these environment variables:

```
CMS_ALERTS_WEBHOOK=<your alerts webhook>
CMS_HEALTH_WEBHOOK=<your health webhook>
DEEPSEEK_API_KEY=<your key>
CMS_SCAN_INTERVAL=5
CMS_DATA_DIR=/app/data
```

4. Add a **Volume** mounted at `/app/data` for persistence across deploys
5. Deploy!

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CMS_ALERTS_WEBHOOK` | (hardcoded) | Discord webhook for CMS alerts |
| `CMS_HEALTH_WEBHOOK` | (hardcoded) | Discord webhook for health status |
| `DEEPSEEK_API_KEY` | (hardcoded) | DeepSeek API key |
| `CMS_SCAN_INTERVAL` | `5` | Minutes between scan cycles |
| `CMS_DATA_DIR` | `/app/data` | Directory for persistence files |

## Alert Format

Alerts include:
- 📊 **Impact Level** (HIGH = red, MEDIUM = orange, LOW = gold)
- 🏷️ **Category** (New Code, Coverage Change, Product Approval, etc.)
- 🏢 **Companies** affected
- 📈 **Ticker symbols** identified
- 💡 **AI Analysis** summary
- 📝 **Source** link

## Files
- `cms_monitor.py` — Main application
- `Dockerfile` — Container build
- `railway.toml` — Railway deployment config
- `requirements.txt` — Python dependencies
- `.env.example` — Environment variable template
