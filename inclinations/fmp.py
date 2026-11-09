#!/usr/bin/env python3
"""Shared FMP SSE-MCP client helper for inclination sub-agents.

REST API is DEAD (403). This is the ONLY working transport. Import it:
    from fmp import fmp_session, fmp_call

Usage:
    sid = fmp_session()
    prof = fmp_call(sid, "company", {"endpoint":"profile-symbol","symbol":"NVDA"}, 1)

Rules: max ~12 FMP calls per run, 0.4s sleep between calls, retry once on 429.
All FMP numeric fields are FLAT camelCase numbers (not {value} dicts).
"""
import json
import time
import urllib.request

URL = "https://financialmodelingprep.com/mcp?apikey=YFP1FhbI0QC2DC2mNh7InDvWxdCn5Uh9"
_HDRS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


def _post(payload, sid=None, timeout=40):
    h = dict(_HDRS)
    if sid:
        h["Mcp-Session-Id"] = sid
    req = urllib.request.Request(URL, data=json.dumps(payload).encode(), method="POST", headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out_sid = r.headers.get("mcp-session-id") or r.headers.get("Mcp-Session-Id")
        body = r.read().decode("utf-8", "replace")
    return out_sid, body


def _parse(body):
    """SSE or plain JSON -> tool result content[0].text parsed as JSON (or dict)."""
    if body.lstrip().startswith("data:") or "\ndata:" in body or body.startswith("event:"):
        for line in body.splitlines():
            if line.startswith("data:"):
                body = line[5:].strip()
                break
    try:
        d = json.loads(body)
    except Exception:
        return {"_error": "unparseable", "_body": body[:300]}
    res = d.get("result", d)
    content = res.get("content", []) if isinstance(res, dict) else []
    if content and isinstance(content, list) and content[0].get("type") == "text":
        try:
            return json.loads(content[0]["text"])
        except Exception:
            return {"_raw_text": content[0]["text"][:500]}
    return res


def fmp_session():
    sid, body = _post({"jsonrpc": "2.0", "method": "initialize",
                      "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                 "clientInfo": {"name": "inclination", "version": "1"}},
                      "id": 1})
    if not sid:
        try:
            sid = json.loads(body).get("result", {}).get("sessionId")
        except Exception:
            pass
    return sid


def fmp_call(sid, name, arguments, call_id, retry=True):
    """One tools/call. Returns parsed JSON (list/dict) or {'_error': ...}."""
    payload = {"jsonrpc": "2.0", "method": "tools/call",
               "params": {"name": name, "arguments": arguments}, "id": call_id}
    for attempt in range(2 if retry else 1):
        try:
            _, body = _post(payload, sid)
            return _parse(body)
        except Exception as e:
            if "429" in str(e) and retry and attempt == 0:
                time.sleep(2.0)
                continue
            return {"_error": str(e)[:160]}
        time.sleep(0.4)
    return {"_error": "retry-exhausted"}


# ---------------------------------------------------------------- confirmed schemas
# Verified against FMP tools/list on 2026-10-16. Use these exact endpoint enum values.
SCHEMAS = {
    "search": {"endpoints": ["search-company-screener", "search-symbol", "search-name", "search-CIK"],
               "args": ["marketCapMoreThan", "marketCapLowerThan", "sector", "industry", "priceMoreThan",
                        "priceLowerThan", "volumeMoreThan", "avgVolumeMoreThan", "exchange", "country",
                        "isEtf", "isFund", "isActivelyTrading", "limit", "page", "query", "symbol"]},
    "company": {"endpoints": ["profile-symbol", "batch-market-cap", "market-cap", "shares-float", "all-shares-float"],
                "args": ["symbol", "symbols", "limit", "page"]},
    "quote": {"endpoints": ["quote", "batch-quote", "batch-quote-short"],
              "args": ["symbol", "symbols", "short"]},
    "statements": {"endpoints": ["income-statement", "balance-sheet-statement", "cashflow-statement",
                                 "key-metrics", "income-statement-growth", "metrics-ratios"],
                   "args": ["symbol", "period", "limit", "year", "structure"]},
    "earningsTranscript": {"endpoints": ["transcripts-dates-by-symbol", "search-transcripts",
                                         "latest-transcripts", "available-transcript-symbols"],
                           "args": ["symbol", "quarter", "year", "limit"]},
    "secFilings": {"endpoints": ["8k-latest", "search-by-symbol", "search-by-name", "search-by-form-type",
                                 "financials-latest", "sec-company-full-profile"],
                   "args": ["from_date", "to_date", "symbol", "company", "cik", "formType", "limit"]},
    "technicalIndicators": {"endpoints": ["relative-strength-index", "simple-moving-average",
                                          "exponential-moving-average", "standard-deviation"],
                            "args": ["symbol", "timeframe", "periodLength", "from_date", "to_date"]},
    "insiderTrades": {"endpoints": ["latest-insider-trade", "search-insider-trades", "insider-trade-statistics"],
                      "args": ["symbol", "limit", "transactionType", "date", "page"]},
}
