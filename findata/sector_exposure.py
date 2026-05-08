"""
findata.sector_exposure
-----------------------
Curated mapping from sector → macro/geopolitical sensitivities, with
example tickers and suggested GDELT queries.

This is editorial knowledge, not scraped — the file is hand-built and
versioned. Every panel run that consumes this gets the same answer
until we deliberately rev the table. The macro / news analyst should
treat outputs as a structural starting point ("here's how this sector
historically transmits these macro shocks"), not a real-time signal.

Source citations on every entry: where the relationship came from
(textbook / IMF working paper / sell-side note / RBI publication).
The web portal renders these alongside the analysis so the user can
sanity-check the editorial claim against the cited source rather
than treating the model's reasoning as gospel.

Coverage: Nifty 50 (by sector) + S&P 500 GICS sectors. ~35 entries.
Last reviewed: 2026-05-09.
"""

from __future__ import annotations

import logging
import re
from typing import Any


logger = logging.getLogger(__name__)


# Module-level constant so it's accessible to tests and so the web UI
# can render the full table on demand.
TABLE_VERSION = "2026-05-09"


# Per-sector exposure dict. Keys are stable sector slugs.
#
# Each entry:
#   sector_label              — human-readable
#   geographic_focus          — "US" / "IN" / "Global" / etc.
#   macro_sensitivities       — list of named drivers
#   key_relationships         — bullet-style strings explaining how
#                               each driver propagates to revenue,
#                               costs, margin, financing, valuation
#   gdelt_query_suggestions   — natural-language queries the news
#                               analyst can use (NO ticker symbols —
#                               GDELT is keyword-based)
#   example_tickers           — concrete tickers in the sector
#   sources                   — citations: textbook chapters, public
#                               IMF working papers, RBI bulletins, etc.
SECTOR_EXPOSURE: dict[str, dict[str, Any]] = {

    # ─── Indian banking + financials ──────────────────────────────
    "in_private_bank": {
        "sector_label": "Indian private bank",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "RBI repo rate", "10Y G-Sec yield", "INR FX",
            "credit growth", "asset quality (NPA cycle)",
        ],
        "key_relationships": [
            "Rising RBI repo rate → NIM widens initially (assets re-price faster than liabilities), then compresses if loan growth slows",
            "Falling 10Y G-Sec yield → trading book gains on AFS portfolio, but lower long-term loan pricing power",
            "INR weakness → minor direct impact (most balance sheet INR-denominated), but indirect pressure if it triggers RBI tightening",
            "Credit growth >12% YoY → NIM expansion + ROA tailwind, watch for asset-quality lag",
            "Slippage uptick → provisions surge, ROE compresses 200-400 bps in cycle",
        ],
        "gdelt_query_suggestions": [
            "RBI monetary policy repo rate India",
            "Indian banking NPA asset quality",
            "Indian credit growth corporate lending",
        ],
        "example_tickers": ["HDFCBANK.NS", "ICICIBANK.NS", "AXISBANK.NS", "KOTAKBANK.NS", "INDUSINDBK.NS"],
        "sources": [
            "RBI Financial Stability Report (semi-annual)",
            "Mishkin, *The Economics of Money, Banking and Financial Markets* — ch. 9-10",
            "IMF working paper WP/22/235 on EM bank NIM and rate cycles",
        ],
    },

    "in_psu_bank": {
        "sector_label": "Indian public-sector bank",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "RBI repo rate", "Government recapitalisation",
            "Public-sector capex cycle", "Asset quality (NPA cycle)",
        ],
        "key_relationships": [
            "RBI rate hikes — NIM uplift smaller than private peers (cheaper deposit franchise but slower loan re-pricing)",
            "Govt recapitalisation announcements — material book-value support; usually share-price-positive on announcement",
            "Public capex cycle (PSE ordering) — directly drives PSU loan-book growth",
            "Asset-quality cycle lags private banks by 2-4 quarters; recovery surprises typical in late-cycle",
        ],
        "gdelt_query_suggestions": [
            "Indian PSU bank recapitalisation",
            "Indian government infrastructure capex",
            "SBI Bank of Baroda lending",
        ],
        "example_tickers": ["SBIN.NS"],
        "sources": [
            "RBI Trend and Progress of Banking in India report",
            "Reserve Bank of India bulletin (monthly)",
        ],
    },

    "in_nbfc": {
        "sector_label": "Indian NBFC / consumer finance",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "RBI repo rate", "10Y G-Sec yield", "Credit market liquidity",
            "Consumer durables demand",
        ],
        "key_relationships": [
            "10Y G-Sec yield ↑ — wholesale funding cost rises faster than retail loan re-pricing → NIM compression",
            "Credit-market stress (post IL&FS / DHFL types) — NBFCs first to lose access to commercial paper; equity tail-risk",
            "Consumer-durables PMI ↓ — vehicle / consumer-loan disbursement growth slows; book size flatlines",
            "RBI tightening NBFC rules (income recognition, scale-based regulation) — compliance cost up + lending mix shifts",
        ],
        "gdelt_query_suggestions": [
            "Indian NBFC funding crisis liquidity",
            "Bajaj Finance lending consumer credit",
            "RBI NBFC regulation",
        ],
        "example_tickers": ["BAJFINANCE.NS", "BAJAJFINSV.NS", "SHRIRAMFIN.NS"],
        "sources": [
            "RBI Report on Trend and Progress of NBFCs",
            "ICRA Indian NBFC sector outlook (annual)",
        ],
    },

    # ─── Indian energy + materials ────────────────────────────────
    "in_refining_oil_marketing": {
        "sector_label": "Indian oil refining + marketing (OMC)",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Crude oil price (WTI/Brent)", "Singapore refining margins (GRM)",
            "INR FX", "Iran/Russia sanctions on crude exports",
            "Strait of Hormuz / Bab el-Mandeb shipping disruption",
            "Indian government retail price intervention",
        ],
        "key_relationships": [
            "Brent +$10/bbl — refining input cost up; GRM widens or compresses depending on cracks (gasoline + diesel) lag",
            "Hormuz tanker disruption — premium on Middle-East-sourced crude (key supply for IOC/BPCL); GRM volatility spikes",
            "Sanctions tighter on Russian Urals — discount narrows; Indian refiners (esp. Reliance, Nayara) lose ~$3-7/bbl arb",
            "INR weakness — input cost up in INR terms; OMCs absorb partially via under-recovery (govt under-recovery socialised)",
            "Retail price intervention pre-election — OMC margin sacrificed; under-recoveries balloon",
        ],
        "gdelt_query_suggestions": [
            "Indian refining margins crude oil",
            "Hormuz strait tanker disruption oil supply",
            "Iran sanctions crude exports India",
            "Russia Urals discount Indian refiners",
        ],
        "example_tickers": ["RELIANCE.NS", "BPCL.NS", "IOC.NS", "ONGC.NS"],
        "sources": [
            "Reliance Industries 20-F (US ADR filing) — risk factors",
            "PPAC (Petroleum Planning and Analysis Cell) monthly bulletin",
            "BCG / Wood Mackenzie Indian refining outlook (sector reports)",
        ],
    },

    "in_metals_mining": {
        "sector_label": "Indian metals + mining",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "China steel demand", "London Metal Exchange prices",
            "Coking coal price", "INR FX",
            "Indian government infrastructure spending",
        ],
        "key_relationships": [
            "China steel demand index ↑ — global HRC prices firm; Indian exporters (Tata Steel, JSW) margin tailwind",
            "Coking coal price ↑ — input cost up materially; integrated producers (with own coal) advantaged",
            "INR weakness — export realisations up (HRC priced in USD), but coking-coal import cost up in tandem",
            "Indian govt capex (railways, roads) — domestic steel demand support; long-products producers benefit",
        ],
        "gdelt_query_suggestions": [
            "China steel demand property crisis",
            "coking coal price Australia exports",
            "Indian infrastructure capex steel",
        ],
        "example_tickers": ["TATASTEEL.NS", "JSWSTEEL.NS", "HINDALCO.NS", "COALINDIA.NS"],
        "sources": [
            "World Steel Association short-range outlook",
            "JPM Indian metals sector quarterly",
        ],
    },

    "in_cement": {
        "sector_label": "Indian cement",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Coal/petcoke price", "Indian housing demand",
            "Indian government infrastructure spending",
            "Diesel price (logistics)",
        ],
        "key_relationships": [
            "Petcoke price ↑ — input cost spikes (cement is energy-intensive); margin compresses 200-500 bps short-term",
            "Housing demand ↓ — bag-cement (retail) volumes hit harder than infra (govt-driven)",
            "Diesel price ↑ — freight a sizeable cost component; ASP doesn't fully absorb",
            "Govt capex acceleration — pricing power supports margin recovery 2-4 quarters out",
        ],
        "gdelt_query_suggestions": [
            "Indian cement demand housing",
            "petcoke coal price India",
            "Indian infrastructure capex government",
        ],
        "example_tickers": ["ULTRACEMCO.NS", "GRASIM.NS"],
        "sources": [
            "CMA (Cement Manufacturers Association) monthly bulletin",
            "Crisil Indian cement outlook (annual)",
        ],
    },

    # ─── Indian IT services + tech ────────────────────────────────
    "in_it_services": {
        "sector_label": "Indian IT services",
        "geographic_focus": "IN/US",   # revenue is US/UK, costs are INR
        "macro_sensitivities": [
            "USD/INR", "US BFSI spending",
            "AI capex / labour-arbitrage erosion",
            "US recession indicators",
        ],
        "key_relationships": [
            "USD/INR weaker (INR depreciation) — revenue translation tailwind; ~50bp margin uplift per 1% INR depreciation",
            "US BFSI tech spend — primary revenue driver (~30-40% of major Indian IT books); sensitive to US bank earnings",
            "AI / GenAI eating into legacy services — pricing pressure on routine maintenance; offset partly by AI-services revenue (still small)",
            "US recession watch — tech budget cuts hit discretionary first; managed-services book is stickier",
        ],
        "gdelt_query_suggestions": [
            "Indian IT services AI revenue impact",
            "US BFSI technology spending budget",
            "TCS Infosys earnings forecast",
        ],
        "example_tickers": ["TCS.NS", "INFY.NS", "WIPRO.NS", "HCLTECH.NS", "TECHM.NS"],
        "sources": [
            "NASSCOM Strategic Review (annual)",
            "Forrester/Gartner enterprise IT spending forecasts",
            "Goldman Sachs Indian IT services sector primer",
        ],
    },

    # ─── Indian autos + auto components ──────────────────────────
    "in_auto_pv_2w": {
        "sector_label": "Indian auto (PV/2W) + auto components",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Steel + aluminium prices", "Crude oil price (fuel)",
            "Indian consumer durables demand", "Semiconductor supply",
            "EV transition pace", "Monsoon (rural demand)",
        ],
        "key_relationships": [
            "Raw material (steel + aluminium) ↑ — input cost up 4-8%; ASP increases lag by a quarter, compressing OEM margin",
            "Crude oil ↑ — fuel cost, dampening discretionary 2W and entry-level PV demand",
            "Monsoon shortfall — rural demand (tractors, 2W) materially hit",
            "Semiconductor allocation — production volumes capped; PV makers prioritised over commercial",
            "EV transition — ICE residual values pressure; capex burden for traditional OEMs",
        ],
        "gdelt_query_suggestions": [
            "Indian auto sales monsoon rural demand",
            "Maruti Tata Motors EV transition",
            "semiconductor shortage automotive",
        ],
        "example_tickers": ["MARUTI.NS", "TATAMOTORS.NS", "M&M.NS", "BAJAJ-AUTO.NS",
                            "EICHERMOT.NS", "HEROMOTOCO.NS"],
        "sources": [
            "SIAM (Society of Indian Automobile Manufacturers) monthly data",
            "ICRA Indian auto sector quarterly",
        ],
    },

    "in_pharma": {
        "sector_label": "Indian pharmaceuticals + generics",
        "geographic_focus": "IN/US",
        "macro_sensitivities": [
            "US FDA approval / warning letters", "US generics pricing",
            "USD/INR", "API supply (China)",
            "Indian regulatory price caps (DPCO)",
        ],
        "key_relationships": [
            "US FDA Form 483 / warning letter — manufacturing site shutdown risk; reputational + cash-flow impact",
            "US generics pricing — multi-year competitive deflation; offset by complex generics + biosimilars",
            "USD/INR weaker — exports to US (~30-50% of revenue) translation tailwind",
            "China API supply disruption — input cost spikes; integrated players advantaged",
            "DPCO list expansion — domestic price caps; revenue impact 1-3% per round",
        ],
        "gdelt_query_suggestions": [
            "FDA warning letter Indian pharma",
            "US generics pricing pressure",
            "China API supply disruption pharmaceuticals",
        ],
        "example_tickers": ["SUNPHARMA.NS", "DRREDDY.NS", "CIPLA.NS", "APOLLOHOSP.NS"],
        "sources": [
            "US FDA Establishment Inspection Reports (public database)",
            "Indian Department of Pharmaceuticals DPCO orders (notifications)",
            "IQVIA Indian pharma outlook",
        ],
    },

    # ─── Indian consumer + telco ─────────────────────────────────
    "in_fmcg": {
        "sector_label": "Indian FMCG / consumer staples",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Rural inflation", "Palm oil price", "Crude oil derivatives",
            "Monsoon", "Indian GDP growth",
        ],
        "key_relationships": [
            "Rural CPI ↑ — premium-product down-trading; volume growth hit",
            "Palm oil ↑ — direct input for soaps/personal care; margin pressure",
            "Crude derivatives — packaging cost (PET/HDPE) tracks crude with lag",
            "Monsoon shortfall — rural FMCG volumes contract 2-5% YoY",
        ],
        "gdelt_query_suggestions": [
            "Indian FMCG rural demand inflation",
            "palm oil price Indonesia Malaysia",
            "Indian monsoon rainfall agriculture",
        ],
        "example_tickers": ["HINDUNILVR.NS", "ITC.NS", "NESTLEIND.NS", "BRITANNIA.NS",
                            "TATACONSUM.NS", "ASIANPAINT.NS"],
        "sources": [
            "IMD (India Meteorological Department) seasonal forecasts",
            "Nielsen India FMCG quarterly volume reports",
        ],
    },

    "in_telecom": {
        "sector_label": "Indian telecom",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Spectrum auction outcomes", "ARPU recovery cycle",
            "5G capex burden", "AGR liabilities",
        ],
        "key_relationships": [
            "Spectrum auction — capex absorbed up-front; later monetised via ARPU step-ups",
            "Tariff hike cycle — direct ARPU lift; Reliance Jio leadership matters",
            "AGR (adjusted gross revenue) Supreme Court ruling — multi-billion-rupee liability for legacy operators",
            "5G capex — heavy 3-5 year investment; FCF compressed, but rural data uplift drives volumes",
        ],
        "gdelt_query_suggestions": [
            "Indian telecom 5G spectrum auction",
            "Bharti Airtel Reliance Jio ARPU",
            "Indian telecom AGR Supreme Court",
        ],
        "example_tickers": ["BHARTIARTL.NS"],
        "sources": [
            "TRAI (Telecom Regulatory Authority of India) quarterly indicators",
            "Department of Telecommunications spectrum auction outcomes",
        ],
    },

    # ─── Indian power + utilities ─────────────────────────────────
    "in_power_gen_utility": {
        "sector_label": "Indian power generation + utility",
        "geographic_focus": "IN",
        "macro_sensitivities": [
            "Coal supply / Coal India production",
            "Imported coal price", "Power demand growth",
            "Renewable capex", "DISCOM payment cycle",
        ],
        "key_relationships": [
            "Coal shortfall — thermal plant load factor drops; PPAs with merchant exposure suffer most",
            "Imported coal ↑ — coastal plants margin hit; mostly pass-through but with lag",
            "Power demand growth (peak) — supports merchant tariffs",
            "Renewable capex — long capex cycle; debt-heavy balance sheets; sensitive to interest-rate environment",
            "DISCOM dues — receivable cycle; periodic central-government settlement schemes",
        ],
        "gdelt_query_suggestions": [
            "Indian power demand coal shortage",
            "Indian renewable energy capex",
            "Coal India production output",
        ],
        "example_tickers": ["NTPC.NS", "POWERGRID.NS", "ADANIENT.NS", "ADANIPORTS.NS"],
        "sources": [
            "Central Electricity Authority (CEA) monthly review",
            "Coal India production data (Ministry of Coal)",
        ],
    },

    # ─── US sectors (selected — extend as needed) ─────────────────
    "us_megacap_tech": {
        "sector_label": "US mega-cap tech (FAAMG-style)",
        "geographic_focus": "US/Global",
        "macro_sensitivities": [
            "10Y Treasury yield (discount rate on long-duration cash flows)",
            "Dollar index (translation drag)",
            "AI capex burden", "Antitrust / regulatory action",
        ],
        "key_relationships": [
            "10Y yield +50bp — long-duration growth multiples compress materially; P/E re-rates 2-4 turns",
            "DXY +5% — international revenue (~50% for big tech) translation drag",
            "AI capex (~$50-100B/yr each for hyperscalers) — FCF compression near-term, monetisation thesis multi-year",
            "Antitrust action (DOJ / EU / India) — fine + structural remedy risk; less material to revenue than to multiple",
        ],
        "gdelt_query_suggestions": [
            "AI capex hyperscaler earnings",
            "Apple Microsoft antitrust",
            "10-year Treasury yield tech valuations",
        ],
        "example_tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA"],
        "sources": [
            "Goldman Sachs US Mega-Cap Tech Outlook (quarterly)",
            "MS / JPM AI capex analysis (sector primers)",
            "Federal Reserve H.15 release (yields)",
        ],
    },

    "us_banks": {
        "sector_label": "US banks (large + regional)",
        "geographic_focus": "US",
        "macro_sensitivities": [
            "Fed Funds rate", "10Y-2Y curve",
            "Commercial real estate (CRE) exposure",
            "Deposit beta", "Credit spreads",
        ],
        "key_relationships": [
            "Curve steepens — NIM expands as long-end re-prices ahead of deposits",
            "Curve inverts — funding cost up, asset yield flat or compressing → NIM compression",
            "CRE valuations down — regional bank charge-offs rise (esp. office sector)",
            "Deposit beta higher than expected — NIM compression; competitive dynamic with money-market funds",
        ],
        "gdelt_query_suggestions": [
            "US bank earnings deposit beta",
            "commercial real estate refinancing wall",
            "Fed Funds rate banking",
        ],
        "example_tickers": ["JPM", "BAC", "WFC", "C", "GS", "MS"],
        "sources": [
            "Federal Reserve H.8 (assets and liabilities of commercial banks)",
            "FDIC Quarterly Banking Profile",
        ],
    },

    "us_energy_e_p": {
        "sector_label": "US energy E&P + integrated oil",
        "geographic_focus": "US/Global",
        "macro_sensitivities": [
            "WTI crude price", "Henry Hub natural gas price",
            "OPEC+ production decisions", "Strategic Petroleum Reserve",
            "Geopolitical supply shocks",
        ],
        "key_relationships": [
            "WTI +$10/bbl — operating cash flow uplift outsized; capex discipline keeps the lift dropping to FCF",
            "OPEC+ cut — supports prices but signals demand softness if accompanied by demand revisions",
            "Hormuz/Russia supply shock — premium on US production; integrated refiners win on cracks",
            "SPR draws — short-term price cap; refill cycle eventually supportive",
        ],
        "gdelt_query_suggestions": [
            "WTI crude OPEC production",
            "Hormuz strait tanker oil disruption",
            "US shale capex production",
        ],
        "example_tickers": ["XOM", "CVX", "COP", "EOG", "OXY"],
        "sources": [
            "EIA Weekly Petroleum Status Report",
            "OPEC Monthly Oil Market Report",
        ],
    },

    "us_consumer_discretionary": {
        "sector_label": "US consumer discretionary (retail + auto)",
        "geographic_focus": "US",
        "macro_sensitivities": [
            "US consumer sentiment", "Real wage growth",
            "Mortgage rate", "Energy prices (gasoline)",
        ],
        "key_relationships": [
            "Consumer sentiment ↓ — discretionary spending compresses; trade-down dynamic",
            "Mortgage rate ↑ — housing-adjacent (furniture, appliances, autos) hit",
            "Gasoline price ↑ — every $0.10/gal absorbs ~$10B/yr from discretionary budget at scale",
        ],
        "gdelt_query_suggestions": [
            "US consumer spending discretionary",
            "US retail earnings inventory",
            "US auto sales pricing",
        ],
        "example_tickers": ["AMZN", "TSLA", "HD", "MCD", "NKE"],
        "sources": [
            "University of Michigan Consumer Sentiment",
            "BLS Consumer Expenditure Survey",
        ],
    },
}


# Plain-prefix → sector slug mapping for tickers we don't enumerate
# explicitly. Order matters — first match wins. Used as a fallback by
# resolve_sector_for_ticker() when the ticker isn't in any sector's
# example_tickers list.
_FALLBACK_BY_SUFFIX: list[tuple[str, str]] = [
    # Indian financials (.NS suffix common bank names)
    (r"^(HDFCLIFE|SBILIFE|HDFC)\.NS$", "in_private_bank"),
    # Indian IT services
    (r"^(?:TCS|INFY|WIPRO|HCLTECH|TECHM|LTIM)\.NS$", "in_it_services"),
    # Indian autos
    (r"^(?:MARUTI|TATAMOTORS|M&M|BAJAJ-AUTO|EICHERMOT|HEROMOTOCO)\.NS$", "in_auto_pv_2w"),
    # Anything .NS not matched above falls through to a generic Indian eq
    (r"\.NS$", "in_fmcg"),  # weak default — at least gives it INR/monsoon context
]


def resolve_sector_for_ticker(ticker: str) -> tuple[str, dict[str, Any]]:
    """Look up the sector entry for a ticker.

    Returns (sector_slug, sector_dict). If the ticker isn't in any
    sector's ``example_tickers`` and doesn't match a fallback regex,
    returns ('unknown', {...stub...}) with empty sensitivities so the
    agent doesn't get false signals — it'll know it has no sector
    map for this name.
    """
    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        raise ValueError("ticker must be non-empty")

    for slug, entry in SECTOR_EXPOSURE.items():
        if ticker_u in entry.get("example_tickers", []):
            return slug, entry

    for pattern, slug in _FALLBACK_BY_SUFFIX:
        if re.search(pattern, ticker_u, re.IGNORECASE):
            return slug, SECTOR_EXPOSURE[slug]

    return ("unknown", {
        "sector_label": "(no curated sector mapping)",
        "geographic_focus": "unknown",
        "macro_sensitivities": [],
        "key_relationships": [
            "No editorial sector mapping for this ticker. The macro analyst should reason from fundamentals + news without sector-specific prior.",
        ],
        "gdelt_query_suggestions": [],
        "example_tickers": [ticker_u],
        "sources": [],
    })


def get_sector_exposure(ticker: str) -> dict[str, Any]:
    """Public entry: return the sector exposure profile for a ticker.

    Always returns a dict with the same shape, including a
    ``sector_slug`` key and the table version so the web portal can
    surface "this is editorial knowledge, last reviewed YYYY-MM-DD"
    for proofreading.
    """
    slug, entry = resolve_sector_for_ticker(ticker)
    out = dict(entry)
    out["ticker"] = ticker.strip().upper()
    out["sector_slug"] = slug
    out["table_version"] = TABLE_VERSION
    out["disclaimer"] = (
        "This is editorial / curated knowledge — a hand-built mapping, "
        "not scraped or live. Use as a structural prior, not a real-time signal. "
        "Treat the cited sources as the authoritative reference."
    )
    return out
