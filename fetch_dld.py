#!/usr/bin/env python3
"""
Daily DLD (Dubai Land Department) open-data fetcher for Mugen Real Estate's
Instagram market post.

Runs in GitHub Actions (full internet access, no login needed - DLD open data
is anonymous). Pulls yesterday's Dubai sales transactions + the same day last
year (for YoY), computes the exact KPIs the DXB Interact recipe used, and
writes:
    data/latest.json               <- what the Claude scheduled task reads
    data/history/YYYY-MM-DD.json   <- daily archive

KPI parity target (DXB Interact, verified 24 Jul 2026):
    total 634 (+15% YoY), median AED 1.15M (+17%), AED 1,650/sqft (+1%)
    off-plan primary (developer) 396 @ 984K, off-plan resale (agents) 54 @ 1.33M,
    ready 184 @ 1.38M, top area Dubai South. Segments must sum to total.

The DLD gateway API is the same backend dubailand.gov.ae/en/open-data uses.
Field names are mapped defensively (candidates lists) and the raw keys of the
first row are logged + embedded in the JSON debug block so mismatches are easy
to fix from the Action log alone.
"""

import json
import statistics
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

GATEWAY = "https://gateway.dubailand.gov.ae/open-data/transactions"
SQM_TO_SQFT = 10.7639104
PAGE_SIZE = 1000
MAX_PAGES = 40  # hard stop: 40k rows/day is far above any real day
WEEKEND_MIN_TOTAL = 50  # below this, the renderer should use the 7-day figures

GULF = timezone(timedelta(hours=4))

# ---- defensive field mapping -------------------------------------------------
CAND = {
    "value": ["TRANS_VALUE", "ACTUAL_WORTH", "actual_worth", "AMOUNT", "TRANS_VALUE_AED"],
    "area_sqm": ["ACTUAL_AREA", "PROCEDURE_AREA", "actual_area", "AREA_SQM", "SIZE_SQM"],
    "group": ["GROUP_EN", "TRANS_GROUP_EN", "trans_group_en", "GROUP"],
    "procedure": ["PROCEDURE_EN", "PROC_NAME_EN", "procedure_name_en", "PROCEDURE"],
    "offplan": ["IS_OFFPLAN_EN", "REG_TYPE_EN", "reg_type_en", "IS_OFF_PLAN_EN", "OFFPLAN"],
    "area_name": ["AREA_EN", "AREA_NAME_EN", "area_name_en", "AREA"],
    "date": ["INSTANCE_DATE", "instance_date", "TRANSACTION_DATE"],
}


def pick(row: dict, kind: str):
    for k in CAND[kind]:
        if k in row and row[k] not in (None, ""):
            return row[k]
    # case-insensitive fallback
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


# ---- API ---------------------------------------------------------------------
def api_page(from_d: date, to_d: date, skip: int, take: int) -> dict:
    body = {
        "P_FROM_DATE": from_d.strftime("%m/%d/%Y"),
        "P_TO_DATE": to_d.strftime("%m/%d/%Y"),
        "P_GROUP_ID": "",
        "P_IS_OFFPLAN": "",
        "P_IS_FREE_HOLD": "",
        "P_USAGE": "",
        "P_AREA_ID": "",
        "P_PROP_TYPE_ID": "",
        "P_TAKE": str(take),
        "P_SKIP": str(skip),
        "P_SORT": "TRANSACTION_NUMBER_ASC",
    }
    req = urllib.request.Request(
        GATEWAY,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) mugen-daily-post/1.0",
            "Origin": "https://dubailand.gov.ae",
            "Referer": "https://dubailand.gov.ae/en/open-data/real-estate-data/",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def extract_rows(payload) -> list:
    """The gateway wraps results; accept the common shapes."""
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


def fetch_range(from_d: date, to_d: date) -> list:
    rows, skip = [], 0
    for _ in range(MAX_PAGES):
        payload = api_page(from_d, to_d, skip, PAGE_SIZE)
        page = extract_rows(payload)
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return rows


# ---- KPI computation ---------------------------------------------------------
def is_sale(row) -> bool:
    g = pick(row, "group")
    if g is None:
        return True  # if no group field, assume the endpoint is sales-only
    return "sale" in str(g).lower() or "sell" in str(g).lower()


def classify(row) -> str:
    """'dev' (off-plan primary), 'agent' (off-plan resale), or 'ready'."""
    off = str(pick(row, "offplan") or "").lower()
    offplan = ("off" in off and "plan" in off) or off in ("1", "true", "yes", "oqood")
    if not offplan:
        return "ready"
    proc = str(pick(row, "procedure") or "").lower()
    if "pre" in proc and "regist" in proc:  # "Sell - Pre registration"
        return "dev"
    return "agent"


def median_or_none(vals):
    vals = [v for v in vals if v]
    return round(statistics.median(vals)) if vals else None


def kpis(rows: list) -> dict:
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


# ---- main --------------------------------------------------------------------
def main():
    # allow an explicit date for backfills/tests: python fetch_dld.py 2026-07-24
    if len(sys.argv) > 1:
        day = date.fromisoformat(sys.argv[1])
    else:
        day = (datetime.now(GULF) - timedelta(days=1)).date()  # yesterday, Gulf time

    day_ly = day.replace(year=day.year - 1)
    week_start = day - timedelta(days=6)

    print(f"Fetching DLD sales for {day} (YoY vs {day_ly}; week {week_start}..{day})")
    rows = fetch_range(day, day)
    print(f"  rows: {len(rows)}")
    if rows:
        print("  first-row keys:", sorted(rows[0].keys()))

    rows_ly = fetch_range(day_ly, day_ly)
    rows_week = fetch_range(week_start, day)
    rows_week_ly = fetch_range(week_start.replace(year=week_start.year - 1),
                               day_ly)

    k, k_ly = kpis(rows), kpis(rows_ly)
    kw, kw_ly = kpis(rows_week), kpis(rows_week_ly)

    seg_sum = sum(k["segments"][s]["count"] for s in k["segments"])
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": day.isoformat(),
        "date_label": day.strftime("%-d %B %Y") if sys.platform != "win32" else day.strftime("%d %B %Y"),
        "use_weekly": k["count"] < WEEKEND_MIN_TOTAL,
        "daily": {
            **k,
            "yoy": {
                "count_pct": pct(k["count"], k_ly["count"]),
                "median_price_pct": pct(k["median_price"], k_ly["median_price"]),
                "median_psf_pct": pct(k["median_psf"], k_ly["median_psf"]),
            },
        },
        "weekly": {
            "label": f"Week of {week_start.strftime('%d %b')} – {day.strftime('%d %b %Y')}",
            **kw,
            "yoy": {
                "count_pct": pct(kw["count"], kw_ly["count"]),
                "median_price_pct": pct(kw["median_price"], kw_ly["median_price"]),
                "median_psf_pct": pct(kw["median_psf"], kw_ly["median_psf"]),
            },
        },
        "debug": {
            "segments_sum_equals_total": seg_sum == k["count"],
            "segments_sum": seg_sum,
            "raw_row_count": len(rows),
            "first_row_keys": sorted(rows[0].keys()) if rows else [],
            "source": "Dubai Land Department open data (gateway.dubailand.gov.ae)",
        },
    }

    Path("data").mkdir(exist_ok=True)
    Path("data/history").mkdir(exist_ok=True)
    Path("data/latest.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    Path(f"data/history/{day.isoformat()}.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False))
    print(json.dumps({k2: v for k2, v in out.items() if k2 != "debug"}, indent=2)[:1500])
    if not rows:
        print("::warning::0 rows returned - check field mapping / endpoint in the log above")


if __name__ == "__main__":
    main()
