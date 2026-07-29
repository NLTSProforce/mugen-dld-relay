#!/usr/bin/env python3
"""
Daily DLD (Dubai Land Department) open-data fetcher for Mugen Real Estate's
Instagram market post.  v2 - self-diagnosing.

Runs in GitHub Actions. Tries, in order:
  1. DLD open-data gateway API (gateway.dubailand.gov.ae, anonymous POST -
     same backend as dubailand.gov.ae/en/open-data)
  2. Dubai Pulse bulk CSV (dld_transactions-open) - official mirror; large
     file, streamed and filtered to just the dates we need.

Always writes data/latest.json (with an "error" block if all sources failed)
and prints a full diagnostic log to stdout; the workflow commits that log to
run.log in the repo so failures can be diagnosed remotely.

Validation reference (DXB Interact, 24 Jul 2026):
  total 634 (+15% YoY), median AED 1.15M (+17%), AED 1,650/sqft (+1%)
  off-plan dev 396 @ 984K, off-plan agents 54 @ 1.33M, ready 184 @ 1.38M
  top area Dubai South; segments must sum to total.
"""

import csv
import io
import json
import re
import statistics
import sys
import traceback
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

GATEWAY = "https://gateway.dubailand.gov.ae/open-data/transactions"
PULSE_DATASET_PAGE = "https://www.dubaipulse.gov.ae/data/dld-transactions/dld_transactions-open"
SQM_TO_SQFT = 10.7639104
PAGE_SIZE = 1000
MAX_PAGES = 40
WEEKEND_MIN_TOTAL = 50
GULF = timezone(timedelta(hours=4))

def log(*a):
    print(*a, flush=True)

# ---- defensive field mapping -------------------------------------------------
CAND = {
    "value": ["TRANS_VALUE", "ACTUAL_WORTH", "actual_worth", "AMOUNT", "TRANS_VALUE_AED"],
    "area_sqm": ["ACTUAL_AREA", "PROCEDURE_AREA", "procedure_area", "actual_area", "AREA_SQM"],
    "group": ["GROUP_EN", "TRANS_GROUP_EN", "trans_group_en", "GROUP"],
    "procedure": ["PROCEDURE_EN", "PROC_NAME_EN", "procedure_name_en", "PROCEDURE"],
    "offplan": ["IS_OFFPLAN_EN", "REG_TYPE_EN", "reg_type_en", "IS_OFF_PLAN_EN", "OFFPLAN"],
    "area_name": ["AREA_EN", "AREA_NAME_EN", "area_name_en", "AREA"],
    "date": ["INSTANCE_DATE", "instance_date", "TRANSACTION_DATE", "transaction_date"],
}

def pick(row: dict, kind: str):
    for k in CAND[kind]:
        if k in row and row[k] not in (None, ""):
            return row[k]
    lower = {k.lower(): v for k, v in row.items()}
    for k in CAND[kind]:
        if k.lower() in lower and lower[k.lower()] not in (None, ""):
            return lower[k.lower()]
    return None

def to_float(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").replace("AED", "").strip())
    except ValueError:
        return None

# ---- HTTP with diagnostics ---------------------------------------------------
def http(url, data=None, headers=None, timeout=120):
    req = urllib.request.Request(url, data=data, headers=headers or {},
                                 method="POST" if data else "GET")
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read(2000).decode(errors="replace")
        except Exception:
            pass
        log(f"HTTP {e.code} from {url}\n  response body: {body[:1500]}")
        raise
    except Exception as e:
        log(f"Request to {url} failed: {type(e).__name__}: {e}")
        raise

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://dubailand.gov.ae",
    "Referer": "https://dubailand.gov.ae/en/open-data/real-estate-data/",
}

# ---- source 1: gateway API ---------------------------------------------------
def gw_page(from_d, to_d, skip, take):
    body = {
        "P_FROM_DATE": from_d.strftime("%m/%d/%Y"), "P_TO_DATE": to_d.strftime("%m/%d/%Y"),
        "P_GROUP_ID": "", "P_IS_OFFPLAN": "", "P_IS_FREE_HOLD": "", "P_USAGE": "",
        "P_AREA_ID": "", "P_PROP_TYPE_ID": "",
        "P_TAKE": str(take), "P_SKIP": str(skip), "P_SORT": "TRANSACTION_NUMBER_ASC",
    }
    h = dict(BROWSER_HEADERS, **{"Content-Type": "application/json"})
    with http(GATEWAY, json.dumps(body).encode(), h) as r:
        raw = r.read().decode()
    log(f"  gateway {from_d}..{to_d} skip={skip}: {len(raw)} bytes")
    return json.loads(raw)

def extract_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("response", "result", "data", "rows", "items", "Table"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                inner = extract_rows(v)
                if inner:
                    return inner
    return []

def fetch_gateway(from_d, to_d):
    rows, skip = [], 0
    for _ in range(MAX_PAGES):
        payload = gw_page(from_d, to_d, skip, PAGE_SIZE)
        page = extract_rows(payload)
        if not page and skip == 0:
            log("  gateway returned no rows; payload sample:",
                json.dumps(payload)[:800])
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return rows

# ---- source 2: Dubai Pulse bulk CSV -----------------------------------------
def date_keys(d: date):
    return {d.strftime(f) for f in ("%d-%m-%Y", "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y")}

def find_pulse_csv_url():
    with http(PULSE_DATASET_PAGE, headers=BROWSER_HEADERS) as r:
        html = r.read().decode(errors="replace")
    links = re.findall(r'https?://[^"\'<> ]+/download/[^"\'<> ]+', html)
    links += re.findall(r'https?://[^"\'<> ]+\.csv[^"\'<> ]*', html)
    log(f"  pulse page: {len(html)} bytes, candidate links: {links[:5]}")
    for l in links:
        if "transaction" in l.lower():
            return l
    return links[0] if links else None

def fetch_pulse(dates_needed):
    """Stream the bulk CSV once; bucket rows for every date in dates_needed."""
    url = find_pulse_csv_url()
    if not url:
        raise RuntimeError("No CSV link found on Dubai Pulse dataset page")
    log(f"  streaming {url}")
    keymap = {}
    for d in dates_needed:
        for k in date_keys(d):
            keymap[k] = d
    buckets = {d: [] for d in dates_needed}
    with http(url, headers=BROWSER_HEADERS, timeout=1800) as r:
        text = io.TextIOWrapper(r, encoding="utf-8", errors="replace")
        reader = csv.DictReader(text)
        n = 0
        for row in reader:
            n += 1
            ds = str(pick(row, "date") or "")[:10]
            d = keymap.get(ds)
            if d:
                buckets[d].append(row)
            if n % 2_000_000 == 0:
                log(f"  ..{n} rows scanned")
        log(f"  pulse scan done: {n} rows; matches: "
            + ", ".join(f"{d}:{len(v)}" for d, v in buckets.items() if v))
    return buckets

# ---- KPI computation ---------------------------------------------------------
def is_sale(row):
    g = pick(row, "group")
    if g is None:
        return True
    return "sale" in str(g).lower() or "sell" in str(g).lower()

def classify(row):
    off = str(pick(row, "offplan") or "").lower()
    offplan = ("off" in off and "plan" in off) or off in ("1", "true", "yes", "oqood")
    if not offplan:
        return "ready"
    proc = str(pick(row, "procedure") or "").lower()
    if "pre" in proc and "regist" in proc:
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
        v = to_float(pick(r, "value"))
        a = to_float(pick(r, "area_sqm"))
        if v:
            prices.append(v)
            total_value += v
        if v and a and a > 1:
            psf.append(v / (a * SQM_TO_SQFT))
        name = pick(r, "area_name")
        if name:
            areas[str(name).strip()] = areas.get(str(name).strip(), 0) + 1
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
    week_days = [week_start + timedelta(days=i) for i in range(7)]          # includes day
    week_days_ly = [week_start_ly + timedelta(days=i) for i in range(7)]    # includes day_ly

    log(f"=== DLD fetch for {day} (YoY {day_ly}; week {week_start}..{day}) ===")

    rows = rows_ly = rows_week = rows_week_ly = None
    source = None
    errors = {}

    # --- source 1: gateway
    try:
        log("[1] gateway API")
        rows = fetch_gateway(day, day)
        if rows:
            log("  first-row keys:", sorted(rows[0].keys()))
        rows_ly = fetch_gateway(day_ly, day_ly)
        rows_week = fetch_gateway(week_start, day)
        rows_week_ly = fetch_gateway(week_start_ly, day_ly)
        source = "gateway.dubailand.gov.ae"
    except Exception:
        errors["gateway"] = traceback.format_exc()
        log("[1] gateway FAILED:\n" + errors["gateway"])

    # --- source 2: Dubai Pulse bulk CSV
    if source is None:
        try:
            log("[2] Dubai Pulse bulk CSV")
            buckets = fetch_pulse(set(week_days + week_days_ly))
            rows = buckets[day]
            if rows:
                log("  first-row keys:", sorted(rows[0].keys()))
            rows_ly = buckets[day_ly]
            rows_week = [r for d in week_days for r in buckets[d]]
            rows_week_ly = [r for d in week_days_ly for r in buckets[d]]
            source = "Dubai Pulse dld_transactions-open"
        except Exception:
            errors["pulse"] = traceback.format_exc()
            log("[2] pulse FAILED:\n" + errors["pulse"])

    Path("data").mkdir(exist_ok=True)
    Path("data/history").mkdir(exist_ok=True)

    if source is None:
        out = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "date": day.isoformat(),
            "error": "All data sources failed - see run.log in repo root",
        }
        Path("data/latest.json").write_text(json.dumps(out, indent=2))
        log("ALL SOURCES FAILED")
        sys.exit(1)

    k, k_ly = kpis(rows), kpis(rows_ly)
    kw, kw_ly = kpis(rows_week), kpis(rows_week_ly)
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
            "source": source,
            "segments_sum_equals_total": seg_sum == k["count"],
            "segments_sum": seg_sum,
            "raw_row_count": len(rows),
            "first_row_keys": sorted(rows[0].keys()) if rows else [],
            "partial_errors": list(errors),
        },
    }
    Path("data/latest.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    Path(f"data/history/{day.isoformat()}.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False))
    log(json.dumps({x: y for x, y in out.items() if x != "debug"}, indent=2)[:1500])
    log("OK")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log("UNHANDLED:\n" + traceback.format_exc())
        sys.exit(1)
