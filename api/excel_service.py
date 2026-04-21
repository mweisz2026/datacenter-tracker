"""
Reads live pricing from U:\Copy of Financings_DATACENTERS.xlsx.
Bloomberg pushes prices into this file automatically.
Cached for 30 seconds to avoid hammering the file on every request.
"""
import os
import openpyxl
import time

EXCEL_PATH = os.environ.get("EXCEL_PATH", r"U:\Copy of Financings_DATACENTERS.xlsx")
SHEET_NAME = "Large Financings"

# Openpyxl column numbers (1-based) for each bond ID
BOND_COL = {
    "beignet":          2,
    "related_bx":       3,
    "vantage":          4,
    "stack_nm":         5,
    "tract":            6,
    "cifr_black_pearl": 7,
    "wulf":             8,
    "flashc":           9,
    "cifr_barber_lake": 10,
    "apld_pf2":         11,
    "apld":             12,
    "voltag":           13,
    "qts":              14,
    # Sub-tranches (Related/BX)
    "related_bx_sd":    16,
    "related_bx_bank":  17,
}

# Row numbers (1-based) — matches spreadsheet layout
ROWS = {
    "price":                    9,
    "ytw":                      10,   # stored as decimal e.g. 0.06256
    "stw_bps":                  11,   # already in bps
    "spread_for_underlying_bps":12,   # already in bps
    "spread_to_underlying_bps": 13,   # already in bps
    "ytc":                      17,   # stored as decimal
    "stc_bps":                  18,   # already in bps
}

CACHE_TTL_SECONDS = 30
_cache = {"data": None, "ts": 0.0, "error": None}


def _safe_float(val):
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def get_live_prices() -> dict:
    """
    Returns dict keyed by bond_id with live pricing fields.
    Reads fresh from Excel at most every 30 seconds; falls back
    to last known good data if the file is locked or unreachable.
    """
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL_SECONDS:
        return {"prices": _cache["data"], "error": None, "cached": True, "as_of": _cache["ts"]}

    try:
        wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, data_only=True)
        ws = wb[SHEET_NAME]

        prices = {}
        for bond_id, col in BOND_COL.items():
            def cell(row_key):
                return _safe_float(ws.cell(row=ROWS[row_key], column=col).value)

            ytw_raw = cell("ytw")
            ytc_raw = cell("ytc")
            stw     = cell("stw_bps")
            stc     = cell("stc_bps")
            price   = cell("price")
            sfub    = cell("spread_for_underlying_bps")
            stub    = cell("spread_to_underlying_bps")

            prices[bond_id] = {
                "price":                       round(price, 4)        if price   is not None else None,
                "ytw_pct":                     round(ytw_raw * 100, 4) if ytw_raw is not None else None,
                "stw_bps":                     round(stw, 2)          if stw     is not None else None,
                "ytc_pct":                     round(ytc_raw * 100, 4) if ytc_raw is not None else None,
                "stc_bps":                     round(stc, 2)          if stc     is not None else None,
                "spread_for_underlying_bps":   round(sfub, 2)         if sfub    is not None else None,
                "spread_to_underlying_bps":    round(stub, 2)         if stub    is not None else None,
            }

        wb.close()
        _cache["data"] = prices
        _cache["ts"]   = now
        _cache["error"] = None
        return {"prices": prices, "error": None, "cached": False, "as_of": now}

    except Exception as e:
        err = str(e)
        _cache["error"] = err
        # Return stale data rather than failing completely
        if _cache["data"]:
            return {"prices": _cache["data"], "error": f"Using cached prices — {err}", "cached": True, "as_of": _cache["ts"]}
        return {"prices": {}, "error": err, "cached": False, "as_of": 0.0}
