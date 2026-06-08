"""
fetch_funding.py  --  Funding signal refresh for Deep Tech Job Board
======================================================================
Searches Google News RSS for recent funding rounds for every company
in the board, extracts structured data via regex, and updates
funding.json (which is inlined into index.html as the FUNDING const).

Usage:
  cd "C:\\Users\\EricEdelstein\\1. Action Work\\physical-ai-jobs"
  python fetch_funding.py

Output:
  funding_results.json  --  raw search hits + extracted fields for review
  funding_patch.js      --  ready-to-paste FUNDING object for index.html

Dependencies: stdlib only (urllib, xml, re, json, time, concurrent.futures)
"""

import json
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ── Company list (mirrors COMPANIES in index.html) ──────────────────────────
COMPANIES = [
    # Physical AI / Materials Science
    {'id': 'albert',         'name': 'Albert'},
    {'id': 'alchemy',        'name': 'Alchemy'},
    {'id': 'artificial',     'name': 'Artificial.io'},
    {'id': 'benchling',      'name': 'Benchling'},
    {'id': 'dotmatics',      'name': 'Dotmatics'},
    {'id': 'labguru',        'name': 'Labguru'},
    {'id': 'sapio',          'name': 'Sapio Sciences'},
    {'id': 'scispot',        'name': 'Scispot'},
    {'id': 'syre',           'name': 'Syre'},
    {'id': 'tetrascience',   'name': 'TetraScience'},
    {'id': 'uncountable',    'name': 'Uncountable'},
    {'id': 'valdera',        'name': 'Valdera'},
    {'id': 'labviva',        'name': 'Labviva'},
    {'id': 'angstrom',       'name': 'Angstrom AI'},
    {'id': 'atinary',        'name': 'Atinary'},
    {'id': 'cradle',         'name': 'Cradle bio'},
    {'id': 'inflowsai',      'name': 'Inflows AI'},
    {'id': 'intellegens',    'name': 'Intellegens'},
    {'id': 'nextmol',        'name': 'NextMol'},
    {'id': 'spaero',         'name': 'Spaero Bio'},
    {'id': 'sunthetics',     'name': 'Sunthetics'},
    {'id': 'quantistry',     'name': 'Quantistry'},
    {'id': 'citrine',        'name': 'Citrine Informatics'},
    {'id': 'avalo',          'name': 'Avalo'},
    {'id': 'nobleai',        'name': 'Noble AI'},
    {'id': 'cuspai',         'name': 'CuspAI'},
    {'id': 'evoscale',       'name': 'EvolutionaryScale'},
    {'id': 'orbital',        'name': 'Orbital Materials'},
    {'id': 'matsnexus',      'name': 'Materials Nexus'},
    {'id': 'radicalai',      'name': 'Radical AI'},
    {'id': 'aionics',        'name': 'Aionics'},
    {'id': 'osmoses',        'name': 'Osmoses'},
    # AI Edge Infrastructure
    {'id': 'skylo',          'name': 'Skylo'},
    {'id': 'ditto',          'name': 'Ditto AI'},
    {'id': 'tarana',         'name': 'Tarana Wireless'},
    {'id': 'efficientcomp',  'name': 'Efficient Computer'},
    {'id': 'armada',         'name': 'Armada'},
    {'id': 'zededa',         'name': 'Zededa'},
    {'id': 'codemetal',      'name': 'CodeMetal'},
    {'id': 'litmus',         'name': 'Litmus'},
    {'id': 'edgeimpulse',    'name': 'Edge Impulse'},
    {'id': 'latentai',       'name': 'Latent AI'},
    # NVentures
    {'id': 'nv-anthropic',   'name': 'Anthropic'},
    {'id': 'nv-openai',      'name': 'OpenAI'},
    {'id': 'nv-xai',         'name': 'xAI Grok'},
    {'id': 'nv-mistral',     'name': 'Mistral AI'},
    {'id': 'nv-cohere',      'name': 'Cohere'},
    {'id': 'nv-perplexity',  'name': 'Perplexity AI'},
    {'id': 'nv-sakana',      'name': 'Sakana AI'},
    {'id': 'nv-coreweave',   'name': 'CoreWeave'},
    {'id': 'nv-lambda',      'name': 'Lambda Labs'},
    {'id': 'nv-crusoe',      'name': 'Crusoe Energy AI'},
    {'id': 'nv-together',    'name': 'Together AI'},
    {'id': 'nv-figure',      'name': 'Figure AI'},
    {'id': 'nv-skild',       'name': 'Skild AI'},
    {'id': 'nv-wayve',       'name': 'Wayve autonomous'},
    {'id': 'nv-waabi',       'name': 'Waabi'},
    {'id': 'nv-nuro',        'name': 'Nuro'},
    {'id': 'nv-cursor',      'name': 'Cursor AI coding'},
    {'id': 'nv-poolside',    'name': 'Poolside AI'},
    {'id': 'nv-scaleai',     'name': 'Scale AI'},
    {'id': 'nv-runway',      'name': 'Runway ML'},
    {'id': 'nv-cfs',         'name': 'Commonwealth Fusion Systems'},
]

# ── Config ───────────────────────────────────────────────────────────────────
WINDOW_DAYS   = 180   # only include rounds announced within this many days
MAX_WORKERS   = 6     # parallel search threads
SLEEP_BETWEEN = 0.4   # seconds between batches to avoid rate-limiting
TODAY         = datetime.now(timezone.utc)
CUTOFF        = TODAY - timedelta(days=WINDOW_DAYS)

# ── Regex patterns ────────────────────────────────────────────────────────────
# Match dollar amounts like $50M, $1.2B, $300 million, $2 billion
AMOUNT_RE = re.compile(
    r'\$\s*(\d+(?:\.\d+)?)\s*(M|B|million|billion)\b',
    re.IGNORECASE
)
# Match series labels
SERIES_RE = re.compile(
    r'\b(seed|pre-?seed|series\s+[A-H][-–I]?\d*|Series\s+[A-H][-–I]?\d*'
    r'|strategic\s+round|growth\s+round|extension|bridge)\b',
    re.IGNORECASE
)
# Match valuation phrases like "at a $4 billion valuation" or "valued at $2B"
VALUATION_RE = re.compile(
    r'(?:at\s+a?\s*|valued\s+at\s+|valuation\s+of\s+)\$\s*(\d+(?:\.\d+)?)\s*(B|billion|T|trillion)\b',
    re.IGNORECASE
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def normalise_amount(num_str, unit_str):
    """Return a display string like '$50M' or '$1.2B'."""
    n = float(num_str)
    u = unit_str.lower()
    if u in ('m', 'million'):
        return f'${n:g}M'
    if u in ('b', 'billion'):
        return f'${n:g}B'
    return f'${num_str}{unit_str}'


def parse_rss_date(s):
    """Parse RFC 2822 date strings from RSS into a UTC datetime (best-effort)."""
    for fmt in ('%a, %d %b %Y %H:%M:%S %z', '%a, %d %b %Y %H:%M:%S GMT'):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def search_funding_news(company_id, company_name):
    """
    Search Google News RSS for recent funding coverage of company_name.
    Returns a list of dicts: {title, link, pub_date, age_days, extracted}.
    extracted = {amount, series, valuation} (all may be None).
    """
    query = urllib.parse.quote(
        f'"{company_name}" funding round million OR billion 2025 OR 2026'
    )
    url = (
        f'https://news.google.com/rss/search?q={query}'
        f'&hl=en-US&gl=US&ceid=US:en'
    )
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0 Safari/537.36'
        )
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            raw = resp.read()
    except Exception as e:
        return {'id': company_id, 'name': company_name, 'error': str(e), 'hits': []}

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        return {'id': company_id, 'name': company_name, 'error': f'xml-{e}', 'hits': []}

    hits = []
    for item in root.findall('.//item')[:8]:
        title   = (item.findtext('title')   or '').strip()
        link    = (item.findtext('link')    or '').strip()
        pub_str = (item.findtext('pubDate') or '').strip()
        desc    = (item.findtext('description') or '').strip()
        full_text = f'{title} {desc}'

        pub_dt  = parse_rss_date(pub_str)
        age_days = (TODAY - pub_dt).days if pub_dt else None

        # Skip articles outside the window
        if age_days is not None and age_days > WINDOW_DAYS:
            continue

        # Extract fields from text
        amount_m   = AMOUNT_RE.search(full_text)
        series_m   = SERIES_RE.search(full_text)
        val_m      = VALUATION_RE.search(full_text)

        extracted = {
            'amount':    normalise_amount(amount_m.group(1), amount_m.group(2)) if amount_m else None,
            'series':    series_m.group(0).replace('-', ' ').title()           if series_m else None,
            'valuation': f'${float(val_m.group(1)):g}{"B" if val_m.group(2).lower() in ("b","billion") else "T"}' if val_m else None,
        }

        hits.append({
            'title':    title,
            'link':     link,
            'pub_date': pub_str,
            'age_days': age_days,
            'extracted': extracted,
        })

    return {'id': company_id, 'name': company_name, 'error': None, 'hits': hits}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'Searching {len(COMPANIES)} companies (window: {WINDOW_DAYS} days)...\n')
    results = []
    funding_patch = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(search_funding_news, co['id'], co['name']): co
            for co in COMPANIES
        }
        done = 0
        for future in as_completed(futures):
            res  = future.result()
            done += 1
            status = 'ERR ' if res['error'] else f'{len(res["hits"])} hit(s)'
            print(f'  [{done:>3}/{len(COMPANIES)}]  {res["name"]:<30}  {status}')
            results.append(res)
            time.sleep(SLEEP_BETWEEN / MAX_WORKERS)  # gentle rate-limiting

    # ── Build funding_patch.js from top hit per company ──────────────────────
    funding_entries = []
    for res in results:
        if res['error'] or not res['hits']:
            continue
        best = res['hits'][0]   # most-recent within window
        ex   = best['extracted']
        if not ex['amount'] and not ex['series']:
            continue             # not enough signal to show a badge

        # Infer ISO date from pub_date (best-effort)
        pub_dt = parse_rss_date(best['pub_date'])
        iso_date = pub_dt.strftime('%Y-%m-%d') if pub_dt else ''

        entry = {
            'id':          res['id'],
            'date':        iso_date,
            'series':      ex['series']    or 'Round',
            'amount':      ex['amount']    or 'Undisclosed',
            'valuation':   ex['valuation'],
            'investors':   [],              # manual fill-in; script can't reliably extract
            'source':      'Google News',
            'source_url':  best['link'],
        }
        funding_entries.append(entry)
        funding_patch[res['id']] = entry

    # ── Write raw results for human review ───────────────────────────────────
    out_json = 'funding_results.json'
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f'\nRaw results saved to: {out_json}')

    # ── Write JS patch ────────────────────────────────────────────────────────
    out_js = 'funding_patch.js'
    lines = ['// Auto-generated by fetch_funding.py — paste into FUNDING const in index.html',
             '// Review funding_results.json first; investor lists need manual fill-in.',
             '']
    for e in funding_entries:
        eid = e['id']
        lines.append(f"  '{eid}': {{")
        lines.append(f"    date: '{e['date']}', series: '{e['series']}', amount: '{e['amount']}',")
        val = f"'{e['valuation']}'" if e['valuation'] else 'null'
        lines.append(f"    valuation: {val},")
        lines.append(f"    investors: [],")
        lines.append(f"    source: '{e['source']}',")
        lines.append(f"    source_url: '{e['source_url']}',")
        lines.append(f"  }},")

    with open(out_js, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Patch written to:   {out_js}')
    print(f'\nFound {len(funding_entries)} companies with recent funding signals.')
    print('Review funding_results.json, then merge funding_patch.js entries into')
    print('the FUNDING const in index.html. Add investor names manually.')


if __name__ == '__main__':
    main()
