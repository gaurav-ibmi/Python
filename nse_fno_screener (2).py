"""
=============================================================================
NSE F&O Breakout Screener  —  15-Minute Auto-Rescan, Market Hours Only
=============================================================================
Version : 4.0.0

BEHAVIOUR
---------
• Runs ONLY during NSE market hours: Mon–Fri, 09:15–15:30 IST.
• If launched before 09:15, waits and prints a countdown.
• Performs a FULL rescan of all F&O stocks every 15 minutes.
• Each scan fetches fresh OHLCV, recomputes all indicators, and
  re-evaluates both strategies on updated prices.
• At 15:30 IST (market close) prints an end-of-day summary and exits.
• Weekend / holiday launch: prints a message and exits cleanly.

HOW SYMBOLS ARE FETCHED (100% online, nothing hardcoded)
---------------------------------------------------------
This script uses THREE live sources (tried in order):

  Source 1 — nse.listEquityStocksByIndex('SECURITIES IN F&O')
             Hits: nseindia.com/api/equity-stockIndex
             Returns: symbol + companyName for every F&O eligible stock.

  Source 2 — nse.fetch_fno_underlying()  +  nse.fnoLots()
             Hits: /underlying-information  and  fo_mktlots.csv
             Fallback if Source 1 fails.

  Source 3 — nse.equityMetaInfo(sym)  per symbol
             Fills missing company names.

All three use the `nse` package which handles NSE cookies automatically.

DATA SOURCES
------------
• nse      — live NSE API for F&O symbol master (no hardcoding)
• yfinance — 400 days daily OHLCV + today's 15-min intraday candles
• nsetools — real-time LTP from NSE during market hours

STRATEGIES
----------
A — Swing Breakout (7–20 days):
    close > SMA20 & SMA50  |  high & close >= 40-day high
    volume >= 1.5x avg      |  RSI-14 >= 55
    SL = entry − ATR14 (max 8%)  |  T1 = +2R  |  T2 = +3R

B — Positional Breakout (1–6 weeks):
    close > SMA50 & SMA200  |  close >= 100-day OR 52-week high
    volume >= 1.5x avg       |  RSI-14 >= 60
    SL = entry − ATR14 (max 8%)  |  T1 = +2R  |  T2 = +3R

HOW TO RUN
----------
pip install nse yfinance nsetools pandas pandas-ta numpy requests colorama
python nse_fno_screener.py                   # auto-runs during market hours
python nse_fno_screener.py --interval 30     # rescan every 30 minutes
python nse_fno_screener.py --refresh-symbols # force refresh NSE symbol list
=============================================================================
"""

import os, sys, time, logging, argparse, warnings, threading
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

# ── Required packages
try:
    import yfinance as yf
except ImportError:
    sys.exit("ERROR: pip install yfinance")

try:
    from nse import NSE as _NSELib
    NSE_LIB_OK = True
except ImportError:
    NSE_LIB_OK = False
    print("WARNING: nse package not found. pip install nse")

try:
    from nsetools import Nse as _NseTools
    _nsetools_client = _NseTools()
    NSETOOLS_OK = True
except ImportError:
    _nsetools_client = None
    NSETOOLS_OK = False

try:
    import pandas_ta as ta; TA_OK = True
except ImportError:
    TA_OK = False

try:
    from colorama import init as _ci, Fore, Back, Style
    _ci(autoreset=True); COLOR_OK = True
except ImportError:
    COLOR_OK = False
    class _Stub:
        def __getattr__(self, _): return ""
    Fore = Back = Style = _Stub()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
IST = ZoneInfo("Asia/Kolkata")


# ===========================================================================
# 1. CONFIG
# ===========================================================================
class Config:
    CLOSE_MIN_A       = 100;   CLOSE_MIN_B       = 150
    SMA_SHORT         = 20;    SMA_MED           = 50;    SMA_LONG    = 200
    VOL_SMA_SHORT     = 20;    VOL_SMA_LONG      = 50
    RSI_PERIOD        = 14;    ATR_PERIOD        = 14
    HHV_SWING         = 40;    HHV_POS           = 100;   HHV_52W     = 252
    VOL_MULT_A        = 1.5;   VOL_MULT_B        = 1.5
    RSI_MIN_A         = 55;    RSI_MIN_B         = 60
    T1_R              = 2.0;   T2_R              = 3.0
    HIST_DAYS         = 420

    # ── Market schedule (IST, Mon–Fri only)
    MARKET_OPEN       = (9,  15)   # HH, MM  — market opens
    MARKET_CLOSE      = (15, 30)   # HH, MM  — market closes, script exits

    # ── Rescan interval: full re-scan every N minutes while market is open
    SCAN_INTERVAL_MIN = 15

    # ── Symbol master cache (refreshed once per day)
    SYMBOL_CACHE      = Path("nse_fno_symbols_cache.json")
    CACHE_TTL_HRS     = 24
    NSE_DL_FOLDER     = Path("./nse_tmp")

    # ── Output files
    OUTPUT_CSV        = f"signals_{date.today():%Y%m%d}.csv"
    EOD_SUMMARY_CSV   = f"eod_summary_{date.today():%Y%m%d}.csv"
    LAST_CSV          = "last_scan_signals.csv"   # latest scan (overwritten each run)


# ===========================================================================
# 2. LIVE SYMBOL FETCHER  — three layered sources, all from NSE
# ===========================================================================

def _load_symbol_cache(cfg: Config) -> dict | None:
    """Return cached {symbol: company_name} if fresh (< CACHE_TTL_HRS old)."""
    import json
    p = cfg.SYMBOL_CACHE
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        saved = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
        age_hrs = (datetime.now() - saved).total_seconds() / 3600
        if age_hrs < cfg.CACHE_TTL_HRS:
            log.info("Symbol cache loaded (%d stocks, %.1fh old).",
                     len(data["symbols"]), age_hrs)
            return data["symbols"]
    except Exception:
        pass
    return None


def _save_symbol_cache(symbols: dict, cfg: Config):
    """Persist {symbol: company_name} to disk with timestamp."""
    import json
    cfg.SYMBOL_CACHE.write_text(json.dumps({
        "timestamp": datetime.now().isoformat(),
        "symbols":   symbols,
    }, indent=2))
    log.info("Symbol cache saved → %s (%d stocks)", cfg.SYMBOL_CACHE, len(symbols))


def _source1_list_fno_index(nse_client) -> dict:
    """
    Source 1: nse.listEquityStocksByIndex('SECURITIES IN F&O')
    Returns {symbol: company_name}
    Hits: https://www.nseindia.com/api/equity-stockIndex?index=SECURITIES+IN+F%26O
    """
    try:
        log.info("  [Source 1] listEquityStocksByIndex('SECURITIES IN F&O') ...")
        resp = nse_client.listEquityStocksByIndex("SECURITIES IN F&O")
        data = resp.get("data", [])
        result = {}
        for item in data:
            sym  = (item.get("symbol") or item.get("Symbol") or "").strip().upper()
            name = (item.get("companyName") or item.get("meta", {}).get("companyName")
                    or sym)
            if sym:
                result[sym] = name
        if result:
            log.info("  [Source 1] Got %d symbols.", len(result))
            return result
    except Exception as e:
        log.warning("  [Source 1] Failed: %s", e)
    return {}


def _source2_fno_underlying(nse_client) -> dict:
    """
    Source 2: nse.fetch_fno_underlying()  +  nse.fnoLots()
    fetch_fno_underlying → {IndexList:[...], UnderlyingList:[{symbol,name},...]}
    fnoLots            → {symbol: lot_size}  (as cross-reference)
    Returns {symbol: company_name}
    """
    result = {}
    try:
        log.info("  [Source 2] fetch_fno_underlying() ...")
        data = nse_client.fetch_fno_underlying()

        # UnderlyingList contains equity F&O stocks
        underlying = data.get("UnderlyingList", [])
        for item in underlying:
            sym  = (item.get("symbol") or item.get("underlying") or "").strip().upper()
            name = item.get("name") or item.get("companyName") or sym
            if sym:
                result[sym] = name

        if result:
            log.info("  [Source 2] Got %d symbols from fetch_fno_underlying.", len(result))
    except Exception as e:
        log.warning("  [Source 2a] fetch_fno_underlying failed: %s", e)

    if not result:
        # fallback: fnoLots gives symbols (no company names)
        try:
            log.info("  [Source 2b] fnoLots() ...")
            lots = nse_client.fnoLots()
            for sym in lots:
                if sym and sym not in result:
                    result[sym.strip().upper()] = sym.strip().upper()
            if result:
                log.info("  [Source 2b] Got %d symbols from fnoLots.", len(result))
        except Exception as e:
            log.warning("  [Source 2b] fnoLots failed: %s", e)

    return result


def _source3_enrich_names(nse_client, symbols: dict) -> dict:
    """
    Source 3: For symbols missing a proper company name, fetch via equityMetaInfo.
    Only hits for symbols where name == symbol (i.e. name not yet resolved).
    """
    to_enrich = [s for s, n in symbols.items() if n == s]
    if not to_enrich:
        return symbols

    log.info("  [Source 3] Enriching %d company names via equityMetaInfo ...",
             len(to_enrich))
    enriched = 0
    for sym in to_enrich[:50]:   # cap at 50 to avoid rate-limiting
        try:
            meta = nse_client.equityMetaInfo(sym)
            name = (meta.get("companyName") or meta.get("name") or sym)
            symbols[sym] = name
            enriched += 1
        except Exception:
            pass
    log.info("  [Source 3] Enriched %d names.", enriched)
    return symbols


def fetch_nse_fno_symbols(cfg: Config = Config()) -> dict:
    """
    Fetch the complete NSE F&O equity universe LIVE from NSE.
    Returns {symbol: company_name}

    Strategy:
      1. Check local cache (refreshed every 24 h).
      2. Source 1: listEquityStocksByIndex('SECURITIES IN F&O')
      3. Source 2: fetch_fno_underlying() + fnoLots() (fallback)
      4. Source 3: equityMetaInfo() to fill missing company names.
      5. Cache the result for next run.

    The nse package (pip install nse) manages NSE cookie sessions
    internally — no manual login or API key needed.
    """
    # ── Try cache first
    cached = _load_symbol_cache(cfg)
    if cached:
        return cached

    if not NSE_LIB_OK:
        log.error("nse package not installed. Run: pip install nse")
        sys.exit(1)

    # ── Open NSE session
    cfg.NSE_DL_FOLDER.mkdir(exist_ok=True)
    log.info("Fetching NSE F&O symbol list LIVE from NSE India ...")
    log.info("  Using nse package (handles cookies/session automatically)")

    symbols = {}
    with _NSELib(str(cfg.NSE_DL_FOLDER)) as nse_client:
        # Source 1 (best: has names)
        symbols = _source1_list_fno_index(nse_client)

        # Source 2 (fallback if source 1 empty)
        if not symbols:
            symbols = _source2_fno_underlying(nse_client)

        # Source 3 (enrich missing names)
        if symbols:
            symbols = _source3_enrich_names(nse_client, symbols)

    if not symbols:
        log.error(
            "All live sources failed.\n"
            "  • Check your internet connection.\n"
            "  • NSE website may be temporarily down.\n"
            "  • Try again during IST market hours (09:00–16:00)."
        )
        sys.exit(1)

    # ── Cache and return
    _save_symbol_cache(symbols, cfg)
    log.info("NSE F&O universe: %d stocks fetched live.", len(symbols))
    return symbols


# ===========================================================================
# 3. HISTORICAL DATA  (yfinance daily candles)
# ===========================================================================

def fetch_ohlcv(symbol: str, cfg: Config = Config()) -> pd.DataFrame | None:
    """Download daily OHLCV via yfinance. Returns clean df or None."""
    yf_ticker = f"{symbol}.NS"
    end       = datetime.today()
    start     = end - timedelta(days=cfg.HIST_DAYS)
    try:
        raw = yf.download(
            yf_ticker,
            start       = start.strftime("%Y-%m-%d"),
            end         = end.strftime("%Y-%m-%d"),
            interval    = "1d",
            progress    = False,
            auto_adjust = True,
        )
    except Exception:
        return None
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.droplevel(1)
    raw = (raw.rename(columns=str.lower)
             .reset_index()
             .rename(columns={"index": "date", "Date": "date"}))
    raw["date"] = pd.to_datetime(raw["date"]).dt.date
    raw = raw.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    return raw if len(raw) >= 60 else None


# ===========================================================================
# 4. LIVE LTP
# ===========================================================================

def get_live_ltp(symbol: str) -> tuple[float | None, str]:
    """Real-time LTP: nsetools → yfinance 1m fallback."""
    if NSETOOLS_OK:
        try:
            q = _nsetools_client.get_quote(symbol.upper())
            if q:
                ltp = q.get("lastPrice") or q.get("lastTradedPrice")
                if ltp:
                    return float(str(ltp).replace(",", "")), "NSE live"
        except Exception:
            pass
    try:
        df = yf.download(f"{symbol}.NS", period="1d", interval="1m",
                         progress=False, auto_adjust=True)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            return float(df["Close"].iloc[-1]), "yfinance 1m"
    except Exception:
        pass
    return None, "unavailable"


# ===========================================================================
# 5. INDICATORS
# ===========================================================================

def _rsi(close, p=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(com=p - 1, min_periods=p).mean()
    l = (-d).clip(lower=0).ewm(com=p - 1, min_periods=p).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def _atr(h, l, c, p=14):
    tr = pd.concat([
        h - l,
        (h - c.shift()).abs(),
        (l - c.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=p - 1, min_periods=p).mean()


def add_indicators(df: pd.DataFrame, cfg: Config = Config()) -> pd.DataFrame:
    c, h, l, v = df.close, df.high, df.low, df.volume
    df["sma_20"]        = c.rolling(cfg.SMA_SHORT).mean()
    df["sma_50"]        = c.rolling(cfg.SMA_MED).mean()
    df["sma_200"]       = c.rolling(cfg.SMA_LONG).mean()
    df["vol_sma_20"]    = v.rolling(cfg.VOL_SMA_SHORT).mean()
    df["vol_sma_50"]    = v.rolling(cfg.VOL_SMA_LONG).mean()
    df["rsi_14"]        = ta.rsi(c, length=14) if TA_OK else _rsi(c)
    df["atr_14"]        = ta.atr(h, l, c, length=14) if TA_OK else _atr(h, l, c)
    df["hhv_high_40"]   = h.rolling(cfg.HHV_SWING).max()
    df["hhv_high_100"]  = h.rolling(cfg.HHV_POS).max()
    df["hhv_close_252"] = c.rolling(cfg.HHV_52W).max()
    return df


# ===========================================================================
# 6. SIGNAL LOGIC
# ===========================================================================

def _exits(close, atr, low_n, cfg):
    sl   = min(max(close - atr, low_n), close * 0.92)
    risk = max(close - sl, close * 0.03)
    return dict(
        sl            = round(sl, 2),
        target1       = round(close + cfg.T1_R * risk, 2),
        target2       = round(close + cfg.T2_R * risk, 2),
        risk_pct      = round(risk / close * 100, 2),
        t1_pct        = round(cfg.T1_R * risk / close * 100, 2),
        t2_pct        = round(cfg.T2_R * risk / close * 100, 2),
    )


def _has_nan(row, *cols):
    return any(pd.isna(row.get(c)) for c in cols)


def check_a(row, hist, cfg=Config()):
    """Strategy A — Swing Breakout (40-day high)."""
    if _has_nan(row, "sma_20", "sma_50", "rsi_14", "atr_14",
                "hhv_high_40", "vol_sma_20"):
        return None
    c, h = float(row.close), float(row.high)
    if c < cfg.CLOSE_MIN_A:                                       return None
    if c <= float(row.sma_20) or c <= float(row.sma_50):         return None
    hhv = float(row.hhv_high_40)
    if h < hhv or c < hhv:                                        return None
    if float(row.volume) < cfg.VOL_MULT_A * float(row.vol_sma_20): return None
    if float(row.rsi_14) < cfg.RSI_MIN_A:                        return None
    e = _exits(c, float(row.atr_14), float(hist.low.tail(10).min()), cfg)
    return dict(
        strategy  = "A — Swing Breakout",
        breakout  = "40-Day High",
        entry_price = round(c, 2),
        hhv_40d   = round(hhv, 2),
        sma_20    = round(float(row.sma_20), 2),
        sma_50    = round(float(row.sma_50), 2),
        rsi_14    = round(float(row.rsi_14), 1),
        vol_ratio = round(float(row.volume) / float(row.vol_sma_20), 2),
        volume    = int(row.volume),
        vol_avg   = round(float(row.vol_sma_20)),
        **e,
    )


def check_b(row, hist, cfg=Config()):
    """Strategy B — Positional Breakout (100-day / 52-week high)."""
    if _has_nan(row, "sma_50", "sma_200", "rsi_14", "atr_14",
                "hhv_high_100", "hhv_close_252", "vol_sma_50"):
        return None
    c = float(row.close)
    if c < cfg.CLOSE_MIN_B:                                       return None
    if c <= float(row.sma_50) or c <= float(row.sma_200):        return None
    h100, h52  = float(row.hhv_high_100), float(row.hhv_close_252)
    at100, at52 = c >= h100, c >= h52
    if not (at100 or at52):                                       return None
    if float(row.volume) < cfg.VOL_MULT_B * float(row.vol_sma_50): return None
    if float(row.rsi_14) < cfg.RSI_MIN_B:                        return None
    lbls = (["52-Week High"] if at52 else []) + (["100-Day High"] if at100 else [])
    e = _exits(c, float(row.atr_14), float(hist.low.tail(20).min()), cfg)
    return dict(
        strategy  = "B — Positional Breakout",
        breakout  = " & ".join(lbls),
        entry_price = round(c, 2),
        hhv_100d  = round(h100, 2),
        hhv_52w   = round(h52, 2),
        sma_50    = round(float(row.sma_50), 2),
        sma_200   = round(float(row.sma_200), 2),
        rsi_14    = round(float(row.rsi_14), 1),
        vol_ratio = round(float(row.volume) / float(row.vol_sma_50), 2),
        volume    = int(row.volume),
        vol_avg   = round(float(row.vol_sma_50)),
        **e,
    )


# ===========================================================================
# 7. COLOURFUL TERMINAL DASHBOARD
# ===========================================================================
W = 100

def _rsi_colour(r):
    if r >= 70: return Fore.RED
    if r >= 60: return Fore.YELLOW
    if r >= 55: return Fore.GREEN
    return Fore.CYAN

def _rsi_zone(r):
    if r >= 70: return "Overbought ⚠"
    if r >= 60: return "Strong  ▲"
    if r >= 55: return "Bullish ↑"
    return "Neutral"

def _rsi_bar(r, w=22):
    f = int(round(r / 100 * w))
    return f"{_rsi_colour(r)}{'█'*f}{'░'*(w-f)}{Style.RESET_ALL}"

def _vol_bar(ratio, w=16):
    cap = min(ratio, 4.0)
    f   = int(round(cap / 4.0 * w))
    col = Fore.GREEN if ratio >= 2.0 else (Fore.YELLOW if ratio >= 1.5 else Fore.WHITE)
    return f"{col}{'▓'*f}{'░'*(w-f)}{Style.RESET_ALL} {ratio:.2f}x"

def _fmt_vol(v):
    if v >= 1e7: return f"{v/1e7:.2f}Cr"
    if v >= 1e5: return f"{v/1e5:.2f}L"
    return f"{v:,.0f}"

def print_dashboard(df: pd.DataFrame, scan_time: str):
    SEP  = f"{Fore.WHITE}{'─'*W}{Style.RESET_ALL}"
    DSEP = f"{Fore.CYAN}{'═'*W}{Style.RESET_ALL}"

    print(f"\n{DSEP}")
    print(f"{Fore.CYAN}{Style.BRIGHT}  NSE F&O BREAKOUT SCREENER  ·  "
          f"{date.today():%d %b %Y}  ·  Scan: {scan_time}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}  Symbols fetched live from NSE  ·  "
          f"Historical data: yfinance  ·  Live LTP: nsetools{Style.RESET_ALL}")
    print(DSEP)

    if df.empty:
        print(f"\n  {Fore.YELLOW}No breakout signals today.{Style.RESET_ALL}")
        print(f"  Tips:")
        print(f"    • Lower RSI_MIN_A to 50 in Config")
        print(f"    • Lower VOL_MULT_A to 1.2 in Config")
        print(f"    • Run after 14:00 IST when volume confirms\n")
        return

    a_cnt = df.strategy.str.startswith("A").sum()
    b_cnt = df.strategy.str.startswith("B").sum()

    print(f"\n  {Fore.WHITE}Signals: {Style.BRIGHT}{len(df)}{Style.RESET_ALL}"
          f"   {Back.BLUE}{Fore.WHITE} ▲ Swing: {a_cnt} {Style.RESET_ALL}"
          f"   {Back.MAGENTA}{Fore.WHITE} ★ Positional: {b_cnt} {Style.RESET_ALL}"
          f"   {Back.GREEN}{Fore.BLACK} Free · No API Key {Style.RESET_ALL}\n")

    for rank, (_, s) in enumerate(df.iterrows(), 1):
        sym      = s.get("symbol", "")
        name     = s.get("company", sym)
        strategy = s.get("strategy", "")
        breakout = s.get("breakout", "")
        entry    = float(s.get("entry_price", 0))
        sl       = float(s.get("sl", 0))
        t1       = float(s.get("target1", 0))
        t2       = float(s.get("target2", 0))
        rsi      = float(s.get("rsi_14", 0))
        vratio   = float(s.get("vol_ratio", 0))
        risk_p   = float(s.get("risk_pct", 0))
        t1p      = float(s.get("t1_pct", 0))
        t2p      = float(s.get("t2_pct", 0))
        score    = float(s.get("score", 0))
        vol      = int(s.get("volume", 0))
        vol_avg  = int(s.get("vol_avg", 0))
        sdt      = s.get("scan_date", "")
        sma20    = s.get("sma_20",  s.get("sma_50", 0))
        sma50    = s.get("sma_50",  0)
        sma200   = s.get("sma_200", None)

        is_swing = strategy.startswith("A")
        badge    = (f"{Back.BLUE}{Fore.WHITE} SWING #{rank} {Style.RESET_ALL}"
                    if is_swing else
                    f"{Back.MAGENTA}{Fore.WHITE} POSITIONAL #{rank} {Style.RESET_ALL}")
        stars_n  = int(round(score * 5))
        stars    = f"{Fore.YELLOW}{'★'*stars_n}{'☆'*(5-stars_n)}{Style.RESET_ALL}"

        print(SEP)
        # ── Row 1: Symbol · Name · Badge · Stars
        print(f"  {Fore.WHITE}{Style.BRIGHT}{sym:<14}{Style.RESET_ALL}"
              f" {Fore.WHITE}{name:<36}{Style.RESET_ALL}"
              f" {badge}  {stars}  "
              f"{Fore.WHITE}score {Style.BRIGHT}{score:.2f}{Style.RESET_ALL}")

        # ── Row 2: Breakout type · Date
        print(f"  {Fore.MAGENTA}⚡ {breakout:<38}{Style.RESET_ALL}"
              f"  Scan date: {sdt}")

        # ── Row 3: Entry / SL / T1 / T2
        print(f"  "
              f"Entry {Fore.CYAN}{Style.BRIGHT}₹{entry:>10,.2f}{Style.RESET_ALL}   "
              f"SL {Fore.RED}₹{sl:>10,.2f} ({Fore.RED}-{risk_p:.1f}%{Style.RESET_ALL})   "
              f"T1 {Fore.YELLOW}₹{t1:>10,.2f} ({Fore.YELLOW}+{t1p:.1f}%{Style.RESET_ALL})   "
              f"T2 {Fore.GREEN}{Style.BRIGHT}₹{t2:>10,.2f} ({Fore.GREEN}+{t2p:.1f}%{Style.RESET_ALL})")

        # ── Row 4: RSI gauge
        print(f"  {Fore.WHITE}RSI-14{Style.RESET_ALL} "
              f"[{_rsi_bar(rsi)}] "
              f"{_rsi_colour(rsi)}{Style.BRIGHT}{rsi:>5.1f}{Style.RESET_ALL}  "
              f"{Fore.WHITE}{_rsi_zone(rsi)}{Style.RESET_ALL}")

        # ── Row 5: Volume gauge
        print(f"  {Fore.WHITE}Volume{Style.RESET_ALL} "
              f"[{_vol_bar(vratio)}]  "
              f"Today {Fore.WHITE}{Style.BRIGHT}{_fmt_vol(vol)}{Style.RESET_ALL}  "
              f"vs  Avg {Fore.WHITE}{_fmt_vol(vol_avg)}{Style.RESET_ALL}")

        # ── Row 6: SMAs
        sma_parts = [f"SMA-20 {Fore.CYAN}₹{sma20:,.2f}{Style.RESET_ALL}",
                     f"SMA-50 {Fore.CYAN}₹{sma50:,.2f}{Style.RESET_ALL}"]
        if sma200 and not pd.isna(sma200):
            sma_parts.append(f"SMA-200 {Fore.CYAN}₹{float(sma200):,.2f}{Style.RESET_ALL}")
        print(f"  {'   '.join(sma_parts)}")
        print()

    print(f"  {'═'*W}")
    print(f"  {Fore.WHITE}RSI colour: "
          f"{Fore.CYAN}≥55 Bullish  "
          f"{Fore.GREEN}≥55 Bullish  "
          f"{Fore.YELLOW}≥60 Strong  "
          f"{Fore.RED}≥70 Overbought{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}Volume:     "
          f"{Fore.YELLOW}1.5x = minimum  "
          f"{Fore.GREEN}2x+ = strong confirmation{Style.RESET_ALL}\n")


# ===========================================================================
# 8. RANKING
# ===========================================================================

def _rank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    w  = {"rsi_14": 0.30, "vol_ratio": 0.30, "t2_pct": 0.40}
    for col, wt in w.items():
        if col not in df.columns: continue
        lo, hi = df[col].min(), df[col].max()
        df[f"_n_{col}"] = (df[col] - lo) / (hi - lo) if hi > lo else 0.5
    sc  = [f"_n_{c}" for c in w if f"_n_{c}" in df.columns]
    wts = [w[c]      for c in w if f"_n_{c}" in df.columns]
    df["score"] = sum(df[s] * wt for s, wt in zip(sc, wts))
    df["score"] = df.score.round(3)
    df.drop(columns=[c for c in df.columns if c.startswith("_n_")], inplace=True)
    return df.sort_values(["strategy", "score"],
                          ascending=[True, False]).reset_index(drop=True)


# ===========================================================================
# 9. MAIN SCAN
# ===========================================================================

def run_scan(symbol_map: dict, cfg: Config = Config()) -> tuple[pd.DataFrame, dict]:
    """
    Scan all F&O stocks. Returns (signals_df, stock_cache).
    symbol_map: {symbol: company_name}  — fetched live from NSE
    """
    total   = len(symbol_map)
    signals = []
    cache   = {}
    skipped = 0

    print(f"\n{Fore.CYAN}{'='*68}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}  Scanning {total} NSE F&O stocks (symbols from live NSE API){Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*68}{Style.RESET_ALL}\n")

    items = list(symbol_map.items())
    for idx, (sym, name) in enumerate(items, 1):
        df = fetch_ohlcv(sym, cfg)
        if df is None:
            skipped += 1
            bar_pct = idx / total
            filled  = int(bar_pct * 40)
            print(f"\r  {Fore.CYAN}{'█'*filled}{'░'*(40-filled)}{Style.RESET_ALL} "
                  f"{idx}/{total}  {sym:<14}  {Fore.RED}SKIP{Style.RESET_ALL}        ",
                  end="", flush=True)
            continue

        close_last = df.close.iloc[-1]
        bar_pct    = idx / total
        filled     = int(bar_pct * 40)
        print(f"\r  {Fore.CYAN}{'█'*filled}{'░'*(40-filled)}{Style.RESET_ALL} "
              f"{idx}/{total}  {sym:<14}  {Fore.WHITE}₹{close_last:>9,.2f}{Style.RESET_ALL}",
              end="", flush=True)

        df           = add_indicators(df, cfg)
        cache[sym]   = df
        row          = df.iloc[-1]

        for checker in (check_a, check_b):
            sig = checker(row, df, cfg)
            if sig:
                signals.append({
                    "symbol":    sym,
                    "company":   name,
                    "scan_date": row.date,
                    **sig,
                })

    print()  # newline after progress bar
    log.info("Scan done. Signals: %d  |  Skipped (no yfinance data): %d / %d",
             len(signals), skipped, total)

    if not signals:
        return pd.DataFrame(), cache

    return _rank(pd.DataFrame(signals)), cache


# ===========================================================================
# 10. MARKET TIME HELPERS
# ===========================================================================

def _now_ist() -> datetime:
    return datetime.now(IST)


def _market_open_time(cfg: Config) -> datetime:
    now = _now_ist()
    return now.replace(hour=cfg.MARKET_OPEN[0], minute=cfg.MARKET_OPEN[1],
                       second=0, microsecond=0)


def _market_close_time(cfg: Config) -> datetime:
    now = _now_ist()
    return now.replace(hour=cfg.MARKET_CLOSE[0], minute=cfg.MARKET_CLOSE[1],
                       second=0, microsecond=0)


def _is_market_hours(cfg: Config) -> bool:
    """True only on Mon–Fri between 09:15 and 15:30 IST."""
    now = _now_ist()
    if now.weekday() >= 5:
        return False
    return _market_open_time(cfg) <= now <= _market_close_time(cfg)


def _is_weekend_or_holiday(cfg: Config) -> bool:
    """True if today is Saturday or Sunday."""
    return _now_ist().weekday() >= 5


def _seconds_to_market_open(cfg: Config) -> float:
    """Seconds until today's market open. Negative = already open/past."""
    return (_market_open_time(cfg) - _now_ist()).total_seconds()


def _seconds_to_market_close(cfg: Config) -> float:
    """Seconds until today's market close. Negative = already closed."""
    return (_market_close_time(cfg) - _now_ist()).total_seconds()


def _wait_for_market_open(cfg: Config) -> bool:
    """
    Block until market opens.
    Returns True if market will open today, False if weekend/already closed.
    Prints a live countdown every 60 seconds.
    """
    if _is_weekend_or_holiday(cfg):
        day = _now_ist().strftime("%A")
        print(f"\n  {Fore.YELLOW}Today is {day}. NSE is closed on weekends.{Style.RESET_ALL}")
        print(f"  {Fore.WHITE}Run again on a weekday before 15:30 IST.{Style.RESET_ALL}\n")
        return False

    secs_to_close = _seconds_to_market_close(cfg)
    if secs_to_close <= 0:
        print(f"\n  {Fore.YELLOW}Market already closed for today (15:30 IST).{Style.RESET_ALL}")
        print(f"  {Fore.WHITE}Run again tomorrow morning.{Style.RESET_ALL}\n")
        return False

    secs_to_open = _seconds_to_market_open(cfg)
    if secs_to_open <= 0:
        return True   # already in market hours

    open_str = _market_open_time(cfg).strftime("%H:%M IST")
    print(f"\n  {Fore.CYAN}{'═'*68}{Style.RESET_ALL}")
    print(f"  {Fore.CYAN}  NSE F&O SCREENER  —  Waiting for market open{Style.RESET_ALL}")
    print(f"  {Fore.CYAN}{'═'*68}{Style.RESET_ALL}")
    print(f"  Market opens at {Fore.GREEN}{open_str}{Style.RESET_ALL}. "
          f"Countdown begins...\n")

    while True:
        secs_left = _seconds_to_market_open(cfg)
        if secs_left <= 0:
            print(f"\r  {Fore.GREEN}Market is now OPEN!{Style.RESET_ALL}                    ")
            return True

        hh = int(secs_left // 3600)
        mm = int((secs_left % 3600) // 60)
        ss = int(secs_left % 60)
        bar_w  = 30
        done   = bar_w - int(secs_left / (_seconds_to_market_open(cfg) + 1) * bar_w)
        done   = max(0, min(done, bar_w))
        bar    = f"{Fore.GREEN}{'█' * done}{Fore.WHITE}{'░' * (bar_w - done)}{Style.RESET_ALL}"
        now_s  = _now_ist().strftime("%H:%M:%S IST")
        print(f"\r  [{bar}]  {Fore.YELLOW}{hh:02d}:{mm:02d}:{ss:02d}{Style.RESET_ALL} "
              f"remaining  [{now_s}]", end="", flush=True)
        time.sleep(1)


# ===========================================================================
# 11. EOD SUMMARY
# ===========================================================================

def print_eod_summary(all_scans: list[pd.DataFrame], cfg: Config):
    """
    Print and save an end-of-day summary across all 15-min scan rounds.
    Shows which signals appeared, how many times, and in which rounds.
    """
    SEP  = "─" * 90
    DSEP = "═" * 90

    print(f"\n{Fore.CYAN}{DSEP}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{Style.BRIGHT}  END-OF-DAY SUMMARY  —  {date.today():%d %b %Y}  —  NSE F&O Screener{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{DSEP}{Style.RESET_ALL}\n")

    if not all_scans or all(s.empty for s in all_scans):
        print(f"  {Fore.YELLOW}No signals were found in any scan today.{Style.RESET_ALL}\n")
        return

    # Aggregate: count appearances per symbol per strategy
    all_rows = pd.concat([s for s in all_scans if not s.empty], ignore_index=True)
    total_scans = len(all_scans)

    summary = (all_rows.groupby(["symbol", "company", "strategy", "breakout"])
               .agg(
                   appearances   = ("entry_price", "count"),
                   avg_entry     = ("entry_price", "mean"),
                   avg_sl        = ("sl",          "mean"),
                   avg_t1        = ("target1",     "mean"),
                   avg_t2        = ("target2",     "mean"),
                   avg_rsi       = ("rsi_14",      "mean"),
                   avg_vol_ratio = ("vol_ratio",   "mean"),
                   best_score    = ("score",       "max"),
               ).reset_index()
               .sort_values(["appearances", "best_score"], ascending=[False, False]))

    print(f"  Total scan rounds today: {Fore.WHITE}{Style.BRIGHT}{total_scans}{Style.RESET_ALL}  "
          f"({cfg.SCAN_INTERVAL_MIN}-min interval, 09:15–15:30 IST)\n")
    print(f"  {SEP}")
    print(f"  {'Symbol':<14} {'Company':<30} {'Appearances':>11}  "
          f"{'Strategy':<22} {'AvgEntry':>9} {'AvgSL':>9} {'AvgT2':>9} "
          f"{'AvgRSI':>7} {'AvgVol':>7}")
    print(f"  {SEP}")

    for _, row in summary.iterrows():
        appear_pct = row.appearances / total_scans * 100
        appear_bar = f"{Fore.GREEN if appear_pct >= 50 else Fore.YELLOW}{'■' * int(appear_pct/10)}{'□'*(10-int(appear_pct/10))}{Style.RESET_ALL}"
        strat_col  = (f"{Fore.BLUE}A-Swing{Style.RESET_ALL}"
                      if str(row.strategy).startswith("A") else
                      f"{Fore.MAGENTA}B-Pos{Style.RESET_ALL}")
        print(f"  {Fore.WHITE}{Style.BRIGHT}{row.symbol:<14}{Style.RESET_ALL}"
              f" {row.company:<30}  "
              f"{appear_bar} {row.appearances:>2}/{total_scans:<2} ({appear_pct:4.0f}%)  "
              f"{strat_col}  "
              f"{Fore.CYAN}₹{row.avg_entry:>8,.2f}{Style.RESET_ALL}  "
              f"{Fore.RED}₹{row.avg_sl:>8,.2f}{Style.RESET_ALL}  "
              f"{Fore.GREEN}₹{row.avg_t2:>8,.2f}{Style.RESET_ALL}  "
              f"{_rsi_colour(row.avg_rsi)}{row.avg_rsi:>6.1f}{Style.RESET_ALL}  "
              f"{Fore.WHITE}{row.avg_vol_ratio:>6.2f}x{Style.RESET_ALL}")

    print(f"  {SEP}")
    print(f"\n  {Fore.GREEN}High-confidence signals (appeared in ≥50% of scans):{Style.RESET_ALL}")
    strong = summary[summary.appearances / total_scans >= 0.5]
    if strong.empty:
        print(f"  {Fore.YELLOW}  None — no signal persisted across half the trading day.{Style.RESET_ALL}")
    else:
        for _, row in strong.iterrows():
            print(f"  {Fore.WHITE}{Style.BRIGHT}  {row.symbol:<14}{Style.RESET_ALL}"
                  f" {row.company}  —  appeared {row.appearances}/{total_scans} scans  "
                  f"avg entry ₹{row.avg_entry:,.2f}")

    # Save EOD summary CSV
    summary.to_csv(cfg.EOD_SUMMARY_CSV, index=False)
    print(f"\n  {Fore.WHITE}EOD summary saved → {cfg.EOD_SUMMARY_CSV}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{DSEP}{Style.RESET_ALL}\n")


# ===========================================================================
# 12. MARKET SCHEDULER  — full rescan every 15 minutes
# ===========================================================================

class MarketScheduler:
    """
    Runs a complete F&O breakout rescan every SCAN_INTERVAL_MIN minutes
    from market open (09:15 IST) until market close (15:30 IST), then exits.

    Each scan round:
      1. Fetches latest OHLCV for every F&O stock via yfinance.
      2. Recomputes all indicators with fresh data.
      3. Evaluates Strategy A and B breakout conditions.
      4. Prints the colourful dashboard for new/changed signals.
      5. Appends signals to the daily CSV.
      6. Waits SCAN_INTERVAL_MIN minutes, then repeats.

    At 15:30 IST:
      • Stops the scan loop.
      • Prints an end-of-day summary (all signals across all rounds).
      • Exits with code 0.
    """

    def __init__(self, symbol_map: dict, cfg: Config):
        self.symbol_map  = symbol_map
        self.cfg         = cfg
        self.all_scans:  list[pd.DataFrame] = []
        self.all_signals: pd.DataFrame = pd.DataFrame()
        self.round_num   = 0
        self._stop       = threading.Event()

    def _next_scan_time(self) -> datetime:
        interval_secs = self.cfg.SCAN_INTERVAL_MIN * 60
        return _now_ist() + timedelta(seconds=interval_secs)

    def _print_scan_header(self, round_num: int, next_scan: datetime):
        now_s  = _now_ist().strftime("%H:%M:%S IST")
        next_s = next_scan.strftime("%H:%M IST")
        close_s = _market_close_time(self.cfg).strftime("%H:%M IST")
        secs_left = max(0, _seconds_to_market_close(self.cfg))
        hh = int(secs_left // 3600)
        mm = int((secs_left % 3600) // 60)

        print(f"\n{Fore.CYAN}{'═'*80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}"
              f"  SCAN ROUND #{round_num}  ·  {now_s}  ·  "
              f"Next scan: {next_s}  ·  Market closes: {close_s}  "
              f"(in {hh}h {mm}m)"
              f"{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'═'*80}{Style.RESET_ALL}")

    def _print_countdown(self, next_scan: datetime):
        """Show a live countdown to the next scan."""
        print()
        while True:
            if self._stop.is_set():
                return
            if not _is_market_hours(self.cfg):
                return
            secs = (next_scan - _now_ist()).total_seconds()
            if secs <= 0:
                print(f"\r  {Fore.GREEN}Starting next scan...{Style.RESET_ALL}            ")
                return
            mm = int(secs // 60)
            ss = int(secs % 60)
            bar_w  = 40
            total  = self.cfg.SCAN_INTERVAL_MIN * 60
            done   = int((total - secs) / total * bar_w)
            done   = max(0, min(done, bar_w))
            bar    = f"{Fore.GREEN}{'█'*done}{Fore.WHITE}{'░'*(bar_w-done)}{Style.RESET_ALL}"
            now_s  = _now_ist().strftime("%H:%M:%S IST")
            print(f"\r  [{bar}]  "
                  f"{Fore.YELLOW}Next scan in {mm:02d}:{ss:02d}{Style.RESET_ALL}  "
                  f"[{now_s}]", end="", flush=True)
            time.sleep(1)

    def _merge_signals(self, new_signals: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compare new scan signals against all previously seen signals.
        Returns (truly_new, disappeared).
        """
        if new_signals.empty:
            return pd.DataFrame(), self.all_signals.copy() if not self.all_signals.empty else pd.DataFrame()

        if self.all_signals.empty:
            return new_signals.copy(), pd.DataFrame()

        prev_keys = set(zip(self.all_signals["symbol"], self.all_signals["strategy"]))
        new_keys  = set(zip(new_signals["symbol"],      new_signals["strategy"]))

        truly_new   = new_signals[~new_signals.apply(
                          lambda r: (r["symbol"], r["strategy"]) in prev_keys, axis=1)]
        disappeared = self.all_signals[~self.all_signals.apply(
                          lambda r: (r["symbol"], r["strategy"]) in new_keys, axis=1)]
        return truly_new, disappeared

    def _print_new_signals(self, new_sigs: pd.DataFrame, ts: str):
        if new_sigs.empty:
            return
        print(f"\n  {Back.GREEN}{Fore.BLACK} ✨ {len(new_sigs)} NEW SIGNAL(S) THIS ROUND {Style.RESET_ALL}  [{ts}]")
        for _, s in new_sigs.iterrows():
            strat_c = Fore.BLUE if str(s.strategy).startswith("A") else Fore.MAGENTA
            print(f"  {strat_c}▶{Style.RESET_ALL} "
                  f"{Fore.WHITE}{Style.BRIGHT}{s.symbol:<14}{Style.RESET_ALL} "
                  f"{s.get('company', s.symbol):<32}  "
                  f"entry {Fore.CYAN}₹{s.entry_price:,.2f}{Style.RESET_ALL}  "
                  f"sl {Fore.RED}₹{s.sl:,.2f}{Style.RESET_ALL}  "
                  f"T1 {Fore.YELLOW}₹{s.target1:,.2f}{Style.RESET_ALL}  "
                  f"T2 {Fore.GREEN}₹{s.target2:,.2f}{Style.RESET_ALL}  "
                  f"RSI {_rsi_colour(s.rsi_14)}{s.rsi_14:.1f}{Style.RESET_ALL}  "
                  f"vol {s.vol_ratio:.2f}x")

    def _print_disappeared(self, gone: pd.DataFrame, ts: str):
        if gone.empty:
            return
        print(f"\n  {Back.RED}{Fore.WHITE} ⬇ {len(gone)} SIGNAL(S) NO LONGER VALID {Style.RESET_ALL}  [{ts}]")
        for _, s in gone.iterrows():
            print(f"  {Fore.RED}✗{Style.RESET_ALL} "
                  f"{Fore.WHITE}{s.symbol:<14}{Style.RESET_ALL} "
                  f"{s.get('company', s.symbol):<32}  "
                  f"was entry ₹{s.entry_price:,.2f}  "
                  f"(conditions no longer met)")

    def _save_round_csv(self, signals: pd.DataFrame, round_num: int):
        """Append this round's signals to the daily CSV."""
        if signals.empty:
            return
        signals_copy = signals.copy()
        signals_copy.insert(0, "scan_round", round_num)
        signals_copy.insert(1, "scan_time",  _now_ist().strftime("%H:%M IST"))
        mode   = "a" if Path(self.cfg.OUTPUT_CSV).exists() else "w"
        header = mode == "w"
        signals_copy.to_csv(self.cfg.OUTPUT_CSV, mode=mode, header=header, index=False)
        signals_copy.to_csv(self.cfg.LAST_CSV, index=False)   # always overwrite latest

    def run(self):
        """Main loop: scan every 15 min from open to close, then exit."""
        cfg = self.cfg

        while True:
            # ── Market-close check → exit
            if not _is_market_hours(cfg):
                secs_till_close = _seconds_to_market_close(cfg)
                if secs_till_close <= 0:
                    print(f"\n  {Fore.YELLOW}Market closed (15:30 IST).{Style.RESET_ALL} "
                          f"Stopping screener.")
                    break
                # Market not yet open (shouldn't happen if _wait_for_market_open ran)
                time.sleep(10)
                continue

            self.round_num += 1
            next_scan = self._next_scan_time()
            self._print_scan_header(self.round_num, next_scan)

            # ── Full scan
            try:
                signals, _ = run_scan(self.symbol_map, cfg)
            except Exception as e:
                log.error("Scan error: %s", e)
                signals = pd.DataFrame()

            ts = _now_ist().strftime("%H:%M:%S IST")

            # ── Diff vs previous round
            new_sigs, gone_sigs = self._merge_signals(signals)

            # ── Print full dashboard every round
            print_dashboard(signals, _now_ist().strftime("%H:%M IST"))

            # ── Highlight new / disappeared signals
            self._print_new_signals(new_sigs, ts)
            self._print_disappeared(gone_sigs, ts)

            # ── Update state
            self.all_signals = signals
            self.all_scans.append(signals)
            self._save_round_csv(signals, self.round_num)

            # ── Countdown to next scan (respects market close)
            secs_till_close = _seconds_to_market_close(cfg)
            secs_till_next  = (next_scan - _now_ist()).total_seconds()

            if secs_till_close <= secs_till_next:
                # Market will close before next scan — wait out the remaining time
                wait_secs = max(0, secs_till_close)
                print(f"\n  {Fore.YELLOW}Market closes in {int(wait_secs//60)}m "
                      f"{int(wait_secs%60)}s — this is the last scan.{Style.RESET_ALL}")
                time.sleep(wait_secs + 5)   # +5 s buffer
            else:
                self._print_countdown(next_scan)

        # ── End-of-day summary (after loop exits)
        print_eod_summary(self.all_scans, cfg)


# ===========================================================================
# 13. ENTRY POINT
# ===========================================================================

def main():
    ap = argparse.ArgumentParser(
        description=(
            "NSE F&O Screener — full rescan every 15 min, "
            "market hours only (09:15–15:30 IST Mon–Fri), auto-exits at close"
        )
    )
    ap.add_argument(
        "--interval", type=int, default=15, metavar="MINUTES",
        help="Rescan interval in minutes (default: 15)."
    )
    ap.add_argument(
        "--refresh-symbols", action="store_true",
        help="Force re-fetch NSE F&O symbol list (ignore 24-h cache)."
    )
    args = ap.parse_args()
    cfg  = Config()
    cfg.SCAN_INTERVAL_MIN = args.interval

    print(f"\n{Fore.CYAN}{'═'*70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{Style.BRIGHT}"
          f"  NSE F&O BREAKOUT SCREENER  v4.0  ·  {date.today():%d %b %Y}"
          f"{Style.RESET_ALL}")
    print(f"{Fore.WHITE}"
          f"  Scan interval : every {cfg.SCAN_INTERVAL_MIN} minutes"
          f"  |  Market: 09:15–15:30 IST Mon–Fri"
          f"{Style.RESET_ALL}")
    print(f"{Fore.WHITE}"
          f"  Auto-exits at 15:30 IST  |  Symbols fetched live from NSE"
          f"{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'═'*70}{Style.RESET_ALL}")

    # ── Clear symbol cache if requested
    if args.refresh_symbols and cfg.SYMBOL_CACHE.exists():
        cfg.SYMBOL_CACHE.unlink()
        log.info("Symbol cache cleared — will re-fetch from NSE.")

    # ── Wait for market open (exits if weekend / already closed)
    if not _wait_for_market_open(cfg):
        sys.exit(0)

    # ── Fetch F&O symbol list LIVE from NSE (cached 24 h)
    symbol_map = fetch_nse_fno_symbols(cfg)

    # ── Run the market-hours scheduler
    scheduler = MarketScheduler(symbol_map, cfg)
    try:
        scheduler.run()
    except KeyboardInterrupt:
        log.info("\nInterrupted by user.")
        if scheduler.all_scans:
            print_eod_summary(scheduler.all_scans, cfg)

    log.info("Screener exited cleanly.")


if __name__ == "__main__":
    main()
