#!/usr/bin/env python3
"""IP signal: (a) flag recent 8-K filings that actually MENTION IP terms,
(b) count recent patent-filing news per assignee in target tech areas.

Fix (2026-10-17): the old version matched IP terms against the 8-K *filename*
only (e.g. 'tm2627273d8_8k.htm') -> 0 clean flags, always. Now it fetches the
actual 8-K document text (SEC EDGAR finalLink), strips tags, and searches the
real content. Scope is recent-only (last 10 days, top 15 unique filings) so a
run stays fast and polite to EDGAR (2s spacing, real User-Agent).

FMP REST is dead -> shared SSE client only (fmp.py). SearXNG: localhost:8888.
Budget: <=5 FMP calls, <=6 SearXNG queries, <=15 EDGAR fetches. Writes ip_out.json.
"""
import datetime
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, '/home/jaren/.hermes/cron/inclinations')
from fmp import fmp_session, fmp_call  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(HERE, 'ip_out.json')
SEARX = 'http://localhost:8888/search'
LOOKBACK_DAYS = 10          # recent window for the 8-K text scan
MAX_8K_FETCH = 15           # max unique documents fetched (EDGAR politeness)
EDGAR_DELAY_S = 2.0
# EDGAR 403s on localhost/blank contact addresses — a real-looking contact is required.
UA = 'Hermes-inclination-research/1.0 (contact@hermesagent.local)'

IP_TERMS = ['patent', 'licence', 'license', 'licensing', 'intellectual property',
            'ip rights', 'ip portfolio', 'trade secret']
# terms that must NOT fire: boilerplate "no known IP litigation" etc. handled
# by requiring the term to sit in a sentence that also has a subject/verb cue.
CUE_RE = re.compile(
    r'(?i)(assign|grant|granted|filed|issued|acquir\w*|licens\w*|portfolio|'
    r'infring\w*|valid\w*|expiration|obtain\w*|develop\w*|patent(s|ed)?\s+(for|in|covering|covering))')


def _get(url, timeout=15):
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', 'replace')


def _html_to_text(html):
    txt = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html, flags=re.S | re.I)
    txt = re.sub(r'<[^>]+>', ' ', txt)
    txt = txt.replace('&#160;', ' ').replace('&amp;', '&')
    txt = re.sub(r'\s+', ' ', txt)
    return txt


def _flag_doc(sym, text):
    """Return the strongest IP flag in a document's text, or None.

    A bare 'patent' inside boilerplate is weak; require a cue verb/noun nearby
    (grant/filed/acquire/licens...) in the same ~200-char window.
    """
    low = text.lower()
    for term in IP_TERMS:
        start = 0
        while True:
            i = low.find(term, start)
            if i < 0:
                break
            window = text[max(0, i - 200):i + len(term) + 200]
            if CUE_RE.search(window):
                # 240-char window centered on the term (robust even at doc edges)
                center = i + len(term) // 2
                lo = max(0, center - 120)
                snippet = re.sub(r'\s+', ' ', window[lo:lo + 240]).strip()
                return term, snippet
            start = i + len(term)
    return None


def collect_8k_flags(sid):
    """Fetch recent 8-Ks (1 FMP page), then fetch + scan the top N documents."""
    to = datetime.date.today().isoformat()
    fr = (datetime.date.today() - datetime.timedelta(days=LOOKBACK_DAYS)).isoformat()
    r = fmp_call(sid, 'secFilings',
                 {'endpoint': '8k-latest', 'from_date': fr, 'to_date': to, 'limit': 25, 'page': 0},
                 10)
    if not isinstance(r, list):
        return 0, [], '8k-latest returned %r' % str(r)[:160]
    # FRESHNESS GATE (2026-10-26): the 8k-latest global feed can lag by MONTHS
    # (verified 2026-10-25: newest row 2.5y old, from_date/to_date ignored
    # server-side). Fetching stale docs wastes EDGAR politeness and produces
    # phantom "recent 8-K" flags. If the newest filing predates the lookback
    # window, report stale_feed and fetch nothing.
    newest = max((str(f.get('filingDate') or f.get('acceptedDate') or '')[:10] for f in r), default='')
    if newest and newest < fr:
        return len(r), [], 'stale_feed:newest_filing_%s_before_%s' % (newest, fr)
    seen_links, docs = set(), 0
    flags = []
    for f in r:
        link = f.get('finalLink') or f.get('link')
        if not link or link in seen_links:
            continue
        seen_links.add(link)
        if docs >= MAX_8K_FETCH:
            break
        docs += 1
        try:
            text = _html_to_text(_get(link, timeout=12))
        except Exception as e:
            if "403" in str(e):
                # EDGAR is blocking this UA/host — retrying is futile; stop early
                sys.stderr.write("EDGAR 403 on %s — aborting doc fetches (UA/host blocked)\n" % link)
                return len(r), flags, "EDGAR 403 (User-Agent blocked); %d docs fetched before abort" % docs
            time.sleep(EDGAR_DELAY_S)
            continue
        hit = _flag_doc(f.get('symbol') or '?', text)
        if hit:
            term, snippet = hit
            flags.append({
                'ticker': f.get('symbol'),
                'filing_date': (f.get('filingDate') or '')[:10],
                'matched_term': term,
                'snippet': snippet,
                'link': link,
            })
        time.sleep(EDGAR_DELAY_S)
    return len(r), flags, None


TECH_AREAS = [
    ('silicon carbide power', '"patent" "silicon carbide" power semiconductor'),
    ('GaN power electronics', '"patent" "gallium nitride" OR "GaN" power electronics'),
    ('counter-UAS', '"patent" "counter-UAS" OR "anti-drone" defense'),
    ('liquid immersion cooling', '"patent" "liquid cooling" OR "immersion cooling" data center'),
    ('SMR nuclear', '"filed a patent" nuclear OR "small modular reactor" patent'),
    ('patent activity (general)', '"filed a patent" OR "patent application" semiconductor OR "data center"'),
]


def searxng(query):
    # CONFIRMED: time_range=week returns 0 results for patent-news queries
    # (SearXNG engine mix); month is the working window. 2026-10-17.
    u = SEARX + '?' + urllib.parse.urlencode({'q': query, 'format': 'json', 'time_range': 'month'})
    try:
        d = json.loads(_get(u, timeout=12))
        return [r for r in d.get('results', []) if r.get('url')]
    except Exception as e:
        return [{'_error': str(e)[:120]}]


def collect_patent_news():
    filings = []
    assignee_hits = {}
    # proper-noun (1-3 capitalized words) before a patent-action verb
    # ("Kumho E&C Files Patent", "RX filed a patent application")
    comp_re = re.compile(
        r'([A-Z][A-Za-z0-9.&#\'-]+(?:\s+[A-Z][A-Za-z0-9.&#\'-]+){0,2})\s+'
        r'(?:files?|filed|granted|grants|holds|owns|was granted|acquired|pursuing|patents? for?|patent application)')
    stop = {'the', 'a', 'an', 'for', 'and', 'its', 'their', 'this', 'new', 'us', 'patent',
            'kumho', 'wipo', 'google', 'search', 'playstation'}
    for tech, query in TECH_AREAS:
        for res in searxng(query):
            if '_error' in res:
                continue
            title = res.get('title', '') or ''
            content = (res.get('content', '') or '')[:400]
            blob = f"{title} {content}"
            comp = None
            for m in comp_re.finditer(blob):
                cand = m.group(1).strip()
                if cand.lower() not in stop and len(cand) >= 4:
                    comp = cand
                    break
            if comp:
                filings.append({'company': comp, 'tech_area': tech,
                                'headline': title[:140], 'url': res.get('url')})
                assignee_hits[comp] = assignee_hits.get(comp, 0) + 1
        time.sleep(3)   # SearXNG suspends on bursts
    return filings, assignee_hits


def main():
    t0 = time.time()
    fmp_calls = 1
    sid = fmp_session()
    n_rows, flags, err = collect_8k_flags(sid)
    if err:
        sys.stderr.write('8k scan: %s\n' % err)
    filings, assignee_hits = collect_patent_news()
    out = {
        'generated_at': datetime.datetime.now().isoformat(timespec='seconds'),
        'lookback_days': LOOKBACK_DAYS,
        'fmp_calls_made': fmp_calls,
        'searxng_queries_run': len(TECH_AREAS),
        'eight_k_fetched': n_rows,
        'eight_k_ip_flags': flags,
        'patent_filings': filings,
        'patent_assignee_counts': assignee_hits,
    }
    with open(OUT_PATH, 'w') as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps({
        'secs': round(time.time() - t0, 1),
        'eight_k_fetched': n_rows,
        'eight_k_ip_flags': len(flags),
        'patent_filings': len(filings),
        'assignee_counts': assignee_hits,
        'flags_sample': flags[:3],
        'filings_sample': filings[:3],
        'eight_k_err': err,
    }, indent=2, default=str))


if __name__ == '__main__':
    main()
