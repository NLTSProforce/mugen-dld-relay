#!/usr/bin/env python3
"""
Daily DLD (Dubai Land Department) open-data fetcher for Mugen Real Estate's
Instagram market post.  v3 - sources data from Data.Dubai (data.dubai), the
official successor to Dubai Pulse.

How it works (all anonymous, no captcha, no login):
  1. GET https://data.dubai/o/dda/data-services/dataset-download?datasetId=470061
     -> JSON listing the full "Real Estate Transactions" export files
        (e.g. transactions_2026-07-28_18-28-34_0001.csv, ~537 MB x2)
  2. Stream every CSV part, keep only rows whose instance_date (ISO YYYY-MM-DD)
     is one of the ~16 dates we need (target day, same day last year, and both
     trailing 7-day windows), then compute the KPIs.

Verified field names (26 Jul 2026 capture): instance_date, trans_group_en
(Sales/Mortgages), procedure_name_en ("Sell", "Sell - Pre registration",
"Delayed Sell", ...), reg_type_en/reg_type_id, actual_worth, procedure_area,
meter_sale_price, area_name_en.

Always writes data/latest.json (with an "error" block if the source failed);
stdout goes to run.log in the repo via the workflow, so failures are
diagnosable remotely.

Validation reference (DXB Interact, 24 Jul 2026):
  total 634 (+15% YoY), median AED 1.15M (+17%), AED 1,650/sqft (+1%)
  off-plan dev 396 @ 984K, off-plan agents 54 @ 1.33M, ready 184 @ 1.38M
  top area Dubai South; segments must sum to total.
"""

import csv
import io
import json
import statistics
import sys
import traceback
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests  # preinstalled on GitHub Actions runners

PORTAL = "https://data.dubai"
DATASET_ID = 470061  # "Real Estate Transactions" by Dubai Land Department
LIST_URL = f"{PORTAL}/o/dda/data-services/dataset-download?datasetId={DATASET_ID}&page=1&pageSize=30&sortDir=desc"
SQM_TO_SQFT = 10.7639104
WEEKEND_MIN_TOTAL = 50
GULF = timezone(timedelta(hours=4))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{PORTAL}/en/l/{DATASET_ID}",
}


def log(*a):
    print(*a, flush=True)


SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def warm_up():
    """Visit the dataset page first like a normal client, so the session
    carries the portal's cookies before any API call."""
    url = f"{PORTAL}/en/l/{DATASET_ID}"
    r = SESSION.get(url, timeout=120,
                    headers={"Accept": "text/html,application/xhtml+xml"})
    log(f"warm-up GET {url} -> {r.status_code}, {len(r.content)} bytes, "
        f"cookies: {sorted(c.name for c in SESSION.cookies)}")


def http(url, timeout=300, stream=False):
    r = SESSION.get(url, timeout=timeout, stream=stream)
    if r.status_code != 200:
        log(f"HTTP {r.status_code} from {url[:160]}\n  body: {r.text[:1200]}")
        r.raise_for_status()
    return r


# ---- discover export file URLs ----------------------------------------------
def walk_urls(node, found):
    """Recursively collect anything that looks like a URL, with nearby context."""
    if isinstance(node, dict):
        for v in node.values():
            walk_urls(v, found)
    elif isinstance(node, list):
        for v in node:
            walk_urls(v, found)
    elif isinstance(node, str):
        s = node.strip()
        if s.startswith(("http://", "https://")) or (s.startswith("/") and len(s) > 8):
            found.append(s)


def discover_csv_urls():
    r = http(LIST_URL, timeout=120)
    raw = r.text
    log(f"file-list response: {len(raw)} bytes")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        log("file-list response was not JSON; body starts with:\n" + raw[:800])
        raise
    urls = []
    walk_urls(payload, urls)
    # absolutize + dedupe, keep order
    seen, absu = set(), []
    for u in urls:
        full = u if u.startswith("http") else urllib.parse.urljoin(PORTAL, u)
        if full not in seen:
            seen.add(full)
            absu.append(full)
    csvs = [u for u in absu if "csv" in u.lower()]
    log("discovered URLs:", json.dumps([u[:140] for u in absu], indent=1))
    if not csvs:
        # some responses key the format separately; fall back to any non-asset URL
        csvs = [u for u in absu if not any(x in u.lower() for x in (".svg", ".png", ".css", ".js", "license"))]
    if not csvs:
        raise RuntimeError("No CSV export URLs found in dataset-download response "
                           "- see the URL list above; the JSON shape may have changed")
    log(f"using {len(csvs)} CSV file(s)")
    return csvs


# ---- stream + filter ---------------------------------------------------------
def collect_rows(csv_urls, needed: set):
    """Stream each CSV part; bucket rows whose instance_date is in `needed`."""
    buckets = {d: [] for d in needed}
    keys = {d.isoformat(): d for d in needed}
    for url in csv_urls:
        log(f"streaming {url[:140]} ...")
        r = http(url, timeout=3600, stream=True)
        r.raw.decode_content = True
        text = io.TextIOWrapper(r.raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.DictReader(text)
        n = m = 0
        for row in reader:
            n += 1
            d = keys.get(str(row.get("instance_date", ""))[:10])
            if d is not None:
                buckets[d].append(row)
                m += 1
            if n % 2_000_000 == 0:
                log(f"  ..{n:,} rows scanned, {m} matched so far")
        log(f"  done: {n:,} rows, {m} matched in this file")
    log("matches per date: " + ", ".join(
        f"{d.isoformat()}:{len(v)}" for d, v in sorted(buckets.items()) if v))
    return buckets


# ---- KPI computation ---------------------------------------------------------
def to_float(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return None


def is_sale(row):
    return "sale" in str(row.get("trans_group_en", "")).lower()


def classify(row):
    """'dev' (off-plan primary), 'agent' (off-plan resale), or 'ready'."""
    off = str(row.get("reg_type_en", "")).lower()
    offplan = "off" in off and "plan" in off
    if not offplan and str(row.get("reg_type_id", "")).strip() == "1":
        offplan = True  # reg_type_id 1 = off-plan in DLD data
    if not offplan:
        return "ready"
    proc = str(row.get("procedure_name_en", "")).lower()
    if "pre" in proc and "regist" in proc:  # "Sell - Pre registration"
        return "dev"
    return "agent"


def median_or_none(vals):
    vals = [v for v in vals if v]
    return round(statistics.median(vals)) if vals else None


def kpis(rows):
    sales = [r for r in rows if is_sale(r)]
    prices, psf, areas = [], [], {}
    seg = {"dev": [], "agent": [], "ready": []}
    total_value = 0.0
    for r in sales:
        v = to_float(r.get("actual_worth"))
        a = to_float(r.get("procedure_area"))
        if v:
            prices.append(v)
            total_value += v
        if v and a and a > 1:
            psf.append(v / (a * SQM_TO_SQFT))
        name = str(r.get("area_name_en", "")).strip()
        if name:
            areas[name] = areas.get(name, 0) + 1
        seg[classify(r)].append(v)
    return {
        "count": len(sales),
        "median_price": median_or_none(prices),
        "median_psf": median_or_none(psf),
        "total_value": round(total_value),
        "top_area": max(areas, key=areas.get) if areas else None,
        "segments": {
            "offplan_dev": {"count": len(seg["dev"]), "median": median_or_none(seg["dev"])},
            "offplan_agent": {"count": len(seg["agent"]), "median": median_or_none(seg["agent"])},
            "ready": {"count": len(seg["ready"]), "median": median_or_none(seg["ready"])},
        },
    }


def pct(cur, prev):
    if cur is None or not prev:
        return None
    return round((cur - prev) / prev * 100)


def with_yoy(k, k_ly):
    return {**k, "yoy": {
        "count_pct": pct(k["count"], k_ly["count"]),
        "median_price_pct": pct(k["median_price"], k_ly["median_price"]),
        "median_psf_pct": pct(k["median_psf"], k_ly["median_psf"]),
    }}


# ---- main --------------------------------------------------------------------
def main():
    if len(sys.argv) > 1 and sys.argv[1].strip():
        day = date.fromisoformat(sys.argv[1].strip())
    else:
        day = (datetime.now(GULF) - timedelta(days=1)).date()

    day_ly = day.replace(year=day.year - 1)
    week_start = day - timedelta(days=6)
    week_start_ly = week_start.replace(year=week_start.year - 1)
    week_days = [week_start + timedelta(days=i) for i in range(7)]
    week_days_ly = [week_start_ly + timedelta(days=i) for i in range(7)]
    needed = set(week_days + week_days_ly)

    log(f"=== DLD fetch for {day} (YoY {day_ly}; week {week_start}..{day}) ===")
    log(f"source: Data.Dubai dataset {DATASET_ID} (Real Estate Transactions)")

    Path("data").mkdir(exist_ok=True)
    Path("data/history").mkdir(exist_ok=True)

    try:
        warm_up()
        csv_urls = discover_csv_urls()
        buckets = collect_rows(csv_urls, needed)
    except Exception:
        log("FETCH FAILED:\n" + traceback.format_exc())
        Path("data/latest.json").write_text(json.dumps({
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "date": day.isoformat(),
            "error": "Data.Dubai fetch failed - see run.log in repo root",
        }, indent=2))
        sys.exit(1)

    rows = buckets[day]
    if rows:
        log("first matched row keys:", sorted(rows[0].keys()))
    k = kpis(rows)
    k_ly = kpis(buckets[day_ly])
    kw = kpis([r for d in week_days for r in buckets[d]])
    kw_ly = kpis([r for d in week_days_ly for r in buckets[d]])
    seg_sum = sum(k["segments"][s]["count"] for s in k["segments"])

    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": day.isoformat(),
        "date_label": day.strftime("%d %B %Y").lstrip("0"),
        "use_weekly": k["count"] < WEEKEND_MIN_TOTAL,
        "daily": with_yoy(k, k_ly),
        "weekly": {
            "label": f"Week of {week_start.strftime('%d %b')} - {day.strftime('%d %b %Y')}",
            **with_yoy(kw, kw_ly),
        },
        "debug": {
            "source": f"Data.Dubai dataset {DATASET_ID} (DLD Real Estate Transactions)",
            "segments_sum_equals_total": seg_sum == k["count"],
            "segments_sum": seg_sum,
            "raw_rows_for_day": len(rows),
        },
    }
    Path("data/latest.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    Path(f"data/history/{day.isoformat()}.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False))
    log(json.dumps({x: y for x, y in out.items() if x != "debug"}, indent=2)[:1600])
    log("OK")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log("UNHANDLED:\n" + traceback.format_exc())
        sys.exit(1)
