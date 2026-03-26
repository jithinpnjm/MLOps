import os
import numpy as np
import pandas as pd
from datetime import timedelta, time as dt_time, datetime
import time
import math

# IMPORT YOUR AUTHENTICATION MODULE
from fyers_auth import get_fyers_session

# ============================================================================
# CONFIGURATION (SYNCED WITH BACKTEST)
# ============================================================================
START_DATE = "2026-03-23"
END_DATE = "2026-03-23"
OUTPUT_FILE = "api_backtest_result.xlsx"

BASE_CAPITAL = 1800000
MARGIN_MULTIPLIER = 5
TOTAL_CAPITAL_PER_TRADE = BASE_CAPITAL * MARGIN_MULTIPLIER

SL_C1_RANGE_PCT = 0.1
ENTRY_BUFFER_PCT = 0.0125
PROFIT_TRIGGER_RR = 10
TSL_ATR_MULTIPLIER = 5

FORCE_EXIT_TIME = dt_time(15, 0)
MAX_ENTRY_TIME = dt_time(10, 0)
BREAKOUT_LOOKBACK_START = dt_time(9, 30)

TOP_GAINERS_TO_SCAN = 10
TOP_LOSERS_TO_SCAN = 10
TOP_N_STOCKS_TO_TRADE = 1

MIN_ATR_PCT = 2.0
MIN_C1_RANGE_PCT = 1.5
MAX_C1_RANGE_PCT = 10
MIN_AVG_TURNOVER = 500000000
MIN_PREV_DAY_CLOSE = 50

W_C1_RANGE = 0.50
W_CMF = 0.30
W_VOL_SPIKE = 0.20
W_GAP = 0.00

BLACKLIST_STOCKS = [
    "NSE:KEI-EQ", "NSE:TORNTPOWER-EQ", "NSE:SUZLON-EQ", "NSE:PREMIERENE-EQ",
    "NSE:GODREJCP-EQ", "NSE:COROMANDEL-EQ", "NSE:PATANJALI-EQ", "NSE:AUROPHARMA-EQ",
    "NSE:HEROMOTOCO-EQ", "NSE:HDFCAMC-EQ", "NSE:TECHM-EQ", "NSE:INDIGO-EQ",
    "NSE:UNITDSPR-EQ", "NSE:BPCL-EQ", "NSE:DMART-EQ", "NSE:MARICO-EQ",
    "NSE:COLPAL-EQ", "NSE:ZYDUSLIFE-EQ", "NSE:CUMMINSIND-EQ", "NSE:ABCAPITAL-EQ",
    "NSE:ULTRACEMCO-EQ", "NSE:ITCHOTELS-EQ", "NSE:SWIGGY-EQ", "NSE:TRENT-EQ",
    "NSE:PAYTM-EQ", "NSE:INDUSTOWER-EQ", "NSE:CGPOWER-EQ", "NSE:LODHA-EQ",
    "NSE:OBEROIRLTY-EQ", "NSE:MAXHEALTH-EQ", "NSE:M&MFIN-EQ", "NSE:BLUESTARCO-EQ",
    "NSE:BDL-EQ", "NSE:EXIDEIND-EQ", "NSE:PHOENIXLTD-EQ", "NSE:IREDA-EQ",
    "NSE:WAAREEENER-EQ", "NSE:PRESTIGE-EQ", "NSE:IGL-EQ", "NSE:BIOCON-EQ",
    "NSE:PAGEIND-EQ", "NSE:ACC-EQ", "NSE:MPHASIS-EQ", "NSE:NTPCGREEN-EQ",
    "NSE:NYKAA-EQ", "NSE:SAIL-EQ", "NSE:ENRIN-EQ", "NSE:360ONE-EQ",
    "NSE:TIINDIA-EQ", "NSE:HAVELLS-EQ", "NSE:BAJFINANCE-EQ", "NSE:BAJAJHLDNG-EQ",
    "NSE:JSWSTEEL-EQ", "NSE:TATATECH-EQ", "NSE:MAZDOCK-EQ",
    "NSE:NAUKRI-EQ", "NSE:VMM-EQ", "NSE:JSWENERGY-EQ", "NSE:MOTHERSON-EQ",
    "NSE:SHRIRAMFIN-EQ", "NSE:UNIONBANK-EQ", "NSE:SONACOMS-EQ",
    "NSE:BHARTIHEXA-EQ", "NSE:BAJAJFINSV-EQ", "NSE:TORNTPHARM-EQ", "NSE:COFORGE-EQ", "NSE:MOTILALOFS-EQ",
    "NSE:HCLTECH-EQ", "NSE:AUBANK-EQ",
]

MASTER_UNIVERSE = [
    "NSE:NIFTY50-INDEX", "NSE:360ONE-EQ", "NSE:ABB-EQ", "NSE:ABCAPITAL-EQ", "NSE:ACC-EQ", "NSE:ADANIENSOL-EQ",
    "NSE:ADANIENT-EQ", "NSE:ADANIGREEN-EQ", "NSE:ADANIPORTS-EQ", "NSE:ADANIPOWER-EQ", "NSE:ALKEM-EQ",
    "NSE:AMBUJACEM-EQ", "NSE:APLAPOLLO-EQ", "NSE:APOLLOHOSP-EQ", "NSE:ASHOKLEY-EQ", "NSE:ASIANPAINT-EQ",
    "NSE:ASTRAL-EQ", "NSE:ATGL-EQ", "NSE:AUBANK-EQ", "NSE:AUROPHARMA-EQ", "NSE:AXISBANK-EQ",
    "NSE:BAJAJ-AUTO-EQ", "NSE:BAJAJFINSV-EQ", "NSE:BAJAJHFL-EQ", "NSE:BAJAJHLDNG-EQ", "NSE:BAJFINANCE-EQ",
    "NSE:BANKBARODA-EQ", "NSE:BANKINDIA-EQ", "NSE:BDL-EQ", "NSE:BEL-EQ", "NSE:BHARATFORG-EQ",
    "NSE:BHARTIARTL-EQ", "NSE:BHARTIHEXA-EQ", "NSE:BHEL-EQ", "NSE:BIOCON-EQ", "NSE:BLUESTARCO-EQ",
    "NSE:BOSCHLTD-EQ", "NSE:BPCL-EQ", "NSE:BRITANNIA-EQ", "NSE:BSE-EQ", "NSE:CANBK-EQ",
    "NSE:CGPOWER-EQ", "NSE:CHOLAFIN-EQ", "NSE:CIPLA-EQ", "NSE:COALINDIA-EQ", "NSE:COCHINSHIP-EQ",
    "NSE:COFORGE-EQ", "NSE:COLPAL-EQ", "NSE:CONCOR-EQ", "NSE:COROMANDEL-EQ", "NSE:CUMMINSIND-EQ",
    "NSE:DABUR-EQ", "NSE:DIVISLAB-EQ", "NSE:DIXON-EQ", "NSE:DLF-EQ", "NSE:DMART-EQ",
    "NSE:DRREDDY-EQ", "NSE:EICHERMOT-EQ", "NSE:ENRIN-EQ", "NSE:ETERNAL-EQ", "NSE:EXIDEIND-EQ",
    "NSE:FEDERALBNK-EQ", "NSE:FORTIS-EQ", "NSE:GAIL-EQ", "NSE:GLENMARK-EQ", "NSE:GMRAIRPORT-EQ",
    "NSE:GODFRYPHLP-EQ", "NSE:GODREJCP-EQ", "NSE:GODREJPROP-EQ", "NSE:GRASIM-EQ", "NSE:HAL-EQ",
    "NSE:HAVELLS-EQ", "NSE:HCLTECH-EQ", "NSE:HDFCAMC-EQ", "NSE:HDFCBANK-EQ", "NSE:HDFCLIFE-EQ",
    "NSE:HEROMOTOCO-EQ", "NSE:HINDALCO-EQ", "NSE:HINDPETRO-EQ", "NSE:HINDUNILVR-EQ", "NSE:HINDZINC-EQ",
    "NSE:HUDCO-EQ", "NSE:HYUNDAI-EQ", "NSE:ICICIBANK-EQ", "NSE:ICICIGI-EQ", "NSE:IDEA-EQ",
    "NSE:IDFCFIRSTB-EQ", "NSE:IGL-EQ", "NSE:INDHOTEL-EQ", "NSE:INDIANB-EQ", "NSE:INDIGO-EQ",
    "NSE:INDUSINDBK-EQ", "NSE:INDUSTOWER-EQ", "NSE:INFY-EQ", "NSE:IOC-EQ", "NSE:IRB-EQ",
    "NSE:IRCTC-EQ", "NSE:IREDA-EQ", "NSE:IRFC-EQ", "NSE:ITC-EQ", "NSE:ITCHOTELS-EQ",
    "NSE:JINDALSTEL-EQ", "NSE:JIOFIN-EQ", "NSE:JSWENERGY-EQ", "NSE:JSWSTEEL-EQ", "NSE:JUBLFOOD-EQ",
    "NSE:KALYANKJIL-EQ", "NSE:KEI-EQ", "NSE:KOTAKBANK-EQ", "NSE:KPITTECH-EQ", "NSE:LICHSGFIN-EQ",
    "NSE:LICI-EQ", "NSE:LODHA-EQ", "NSE:LT-EQ", "NSE:LTF-EQ", "NSE:LTM-EQ",
    "NSE:LUPIN-EQ", "NSE:M&M-EQ", "NSE:M&MFIN-EQ", "NSE:MANKIND-EQ", "NSE:MARICO-EQ",
    "NSE:MARUTI-EQ", "NSE:MAXHEALTH-EQ", "NSE:MAZDOCK-EQ", "NSE:MFSL-EQ", "NSE:MOTHERSON-EQ",
    "NSE:MOTILALOFS-EQ", "NSE:MPHASIS-EQ", "NSE:MRF-EQ", "NSE:MUTHOOTFIN-EQ", "NSE:NATIONALUM-EQ",
    "NSE:NAUKRI-EQ", "NSE:NESTLEIND-EQ", "NSE:NHPC-EQ", "NSE:NMDC-EQ", "NSE:NTPC-EQ",
    "NSE:NTPCGREEN-EQ", "NSE:NYKAA-EQ", "NSE:OBEROIRLTY-EQ", "NSE:OFSS-EQ", "NSE:OIL-EQ",
    "NSE:ONGC-EQ", "NSE:PAGEIND-EQ", "NSE:PATANJALI-EQ", "NSE:PAYTM-EQ", "NSE:PERSISTENT-EQ",
    "NSE:PFC-EQ", "NSE:PHOENIXLTD-EQ", "NSE:PIDILITIND-EQ", "NSE:PIIND-EQ", "NSE:PNB-EQ",
    "NSE:POLICYBZR-EQ", "NSE:POLYCAB-EQ", "NSE:POWERGRID-EQ", "NSE:POWERINDIA-EQ", "NSE:PREMIERENE-EQ",
    "NSE:PRESTIGE-EQ", "NSE:RECLTD-EQ", "NSE:RELIANCE-EQ", "NSE:RVNL-EQ", "NSE:SAIL-EQ",
    "NSE:SBICARD-EQ", "NSE:SBILIFE-EQ", "NSE:SBIN-EQ", "NSE:SHREECEM-EQ", "NSE:SHRIRAMFIN-EQ",
    "NSE:SIEMENS-EQ", "NSE:SOLARINDS-EQ", "NSE:SONACOMS-EQ", "NSE:SRF-EQ", "NSE:SUNPHARMA-EQ",
    "NSE:SUPREMEIND-EQ", "NSE:SUZLON-EQ", "NSE:SWIGGY-EQ", "NSE:TATACOMM-EQ", "NSE:TATACONSUM-EQ",
    "NSE:TATAELXSI-EQ", "NSE:TATAPOWER-EQ", "NSE:TATASTEEL-EQ", "NSE:TATATECH-EQ", "NSE:TCS-EQ",
    "NSE:TECHM-EQ", "NSE:TIINDIA-EQ", "NSE:TITAN-EQ", "NSE:TMPV-EQ", "NSE:TORNTPHARM-EQ",
    "NSE:TORNTPOWER-EQ", "NSE:TRENT-EQ", "NSE:TVSMOTOR-EQ", "NSE:ULTRACEMCO-EQ", "NSE:UNIONBANK-EQ",
    "NSE:UNITDSPR-EQ", "NSE:UPL-EQ", "NSE:VBL-EQ", "NSE:VEDL-EQ", "NSE:VMM-EQ",
    "NSE:VOLTAS-EQ", "NSE:WAAREEENER-EQ", "NSE:WIPRO-EQ", "NSE:YESBANK-EQ", "NSE:ZYDUSLIFE-EQ"
]


# ============================================================================
# API-BASED BACKTESTER CLASS
# ============================================================================
class FyersAPIBacktester:
    def __init__(self):
        start_time = time.time()
        print("\n⚙️ Initializing Fyers API Backtester...")
        self.fyers = get_fyers_session()

        profile = self.fyers.get_profile()
        if 's' not in profile or profile['s'] != 'ok':
            raise Exception("❌ Fyers Authentication Failed. Check your fyers_auth setup.")

        self.universe = [s for s in MASTER_UNIVERSE if s not in BLACKLIST_STOCKS]
        self.all_daily, self.all_intra, self.trades = {}, {}, []

        # FIX: Use pd.Timestamp for all date arithmetic — avoids datetime.date vs Timestamp mismatch
        fetch_start = pd.to_datetime(START_DATE) - timedelta(days=60)
        fetch_end = pd.to_datetime(END_DATE)

        print(f"📡 Downloading History ({fetch_start.date()} to {fetch_end.date()})...")
        for i, s in enumerate(self.universe, 1):
            daily_df = self._fetch_fyers_history(s, "1D", fetch_start, fetch_end)
            intra_df = self._fetch_fyers_history(s, "15", fetch_start, fetch_end)

            if not daily_df.empty and not intra_df.empty:
                self.all_daily[s], self.all_intra[s] = daily_df, intra_df
                if i % 5 == 0:
                    print(f"   ✅ Fetched {i}/{len(self.universe)} symbols")
            time.sleep(0.1)

        print("\n⚡ Pre-computing Features...")
        all_features = []
        for s in self.all_daily.keys():
            feat = self._precompute_features(s, self.all_daily[s], self.all_intra[s])
            if feat is not None and not feat.empty:
                all_features.append(feat)

        if not all_features:
            print("❌ ERROR: No stocks passed the initial filter. MASTER_FEATURES is empty.")
            self.master_features = pd.DataFrame()
            return

        self.master_features = pd.concat(all_features, ignore_index=True)

        # FIX: Ensure date index is pd.Timestamp, not datetime.date or integer
        self.master_features['date'] = pd.to_datetime(self.master_features['date'])
        self.master_features.set_index('date', inplace=True)
        self.master_features.sort_index(inplace=True)

        print(f"Index dtype: {self.master_features.index.dtype}")
        print(f"Index sample: {self.master_features.index[:3].tolist()}")
        print(f"🚀 Data Prep complete in {time.time() - start_time:.2f} seconds!")

    def _fetch_fyers_history(self, symbol, resolution, start_date, end_date):
        data = {
            "symbol": symbol, "resolution": resolution, "date_format": "1",
            "range_from": start_date.strftime('%Y-%m-%d'),
            "range_to": end_date.strftime('%Y-%m-%d'), "cont_flag": "1"
        }
        res = self.fyers.history(data=data)
        if res.get('s') == 'ok' and res.get('candles'):
            df = pd.DataFrame(res['candles'], columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
            df['datetime'] = (
                pd.to_datetime(df['epoch'], unit='s')
                .dt.tz_localize('UTC')
                .dt.tz_convert('Asia/Kolkata')
                .dt.tz_localize(None)
            )
            return df.sort_values('datetime').reset_index(drop=True)
        return pd.DataFrame()

    def _precompute_features(self, symbol, d_df, i_df):
        if len(d_df) < 25 or i_df.empty:
            return None

        d_df, i_df = d_df.copy(), i_df.copy()

        # FIX: Use pd.Timestamp dates throughout — never datetime.date
        d_df['date'] = pd.to_datetime(d_df['datetime'].dt.date)
        i_df['date'] = pd.to_datetime(i_df['datetime'].dt.date)

        # --- Daily features ---
        d_df['prev_close'] = d_df['close'].shift(1)
        d_df['turnover'] = d_df['close'] * d_df['volume']
        d_df['avg_turnover_20'] = d_df['turnover'].rolling(20, min_periods=20).mean().shift(1)

        tr = pd.concat([
            d_df["high"] - d_df["low"],
            (d_df["high"] - d_df["prev_close"]).abs(),
            (d_df["low"] - d_df["prev_close"]).abs()
        ], axis=1).max(axis=1)
        d_df['atr'] = tr.rolling(14, min_periods=14).mean().shift(1)
        d_df.dropna(subset=['prev_close', 'atr', 'avg_turnover_20'], inplace=True)

        # --- Intraday indicators (computed on full i_df across all dates) ---
        hl_diff = (i_df['high'] - i_df['low']).replace(0, np.nan)
        mfm = (((i_df['close'] - i_df['low']) - (i_df['high'] - i_df['close'])) / hl_diff).fillna(0)
        i_df['cmf'] = (mfm * i_df['volume']).rolling(20).sum() / i_df['volume'].rolling(20).sum()
        i_df['vol_spike'] = i_df['volume'] / i_df['volume'].rolling(19).mean().shift(1)

        # FIX: ffill so the 09:15 candle inherits the last valid value from prior candles
        # Without this, the very first candle of each day has NaN CMF
        i_df['cmf'] = i_df['cmf'].ffill()
        i_df['vol_spike'] = i_df['vol_spike'].ffill()

        # --- DIAGNOSTIC (remove after confirming fix) ---
        if symbol == 'NSE:ATGL-EQ':
            target_date = pd.to_datetime('2026-03-23')
            today_rows = i_df[i_df['date'] == target_date]
            print(f"\n=== ATGL DIAGNOSTIC ===")
            print(f"Total intraday rows for ATGL: {len(i_df)}")
            print(f"Rows on 2026-03-23: {len(today_rows)}")
            print(f"09:15 candles on 2026-03-23:\n{today_rows[today_rows['datetime'].dt.time == dt_time(9,15)][['datetime','cmf','vol_spike']]}")
            print(f"First 3 rows of the day:\n{today_rows[['datetime','cmf','vol_spike']].head(3)}")
            print(f"Last 5 rows before today:\n{i_df[i_df['date'] < target_date][['datetime','cmf','vol_spike']].tail(5)}")
            print(f"=== END DIAGNOSTIC ===\n")

        # C1 OHLCV — first candle of each day (09:15)
        c1_ohlcv = i_df.groupby('date').first().reset_index()
        c1_ohlcv.rename(columns={
            'open': 'c1_open', 'high': 'c1_high', 'low': 'c1_low',
            'close': 'c1_close', 'volume': 'c1_volume'
        }, inplace=True)

        # FIX: Pin indicator values to the exact 09:15 candle per day
        c1_candles = i_df[i_df['datetime'].dt.time == dt_time(9, 15)].copy()
        if c1_candles.empty:
            # Fallback: first candle of each day if 09:15 not present
            c1_indicators = i_df.groupby('date')[['cmf', 'vol_spike']].first().reset_index()
        else:
            c1_indicators = c1_candles.groupby('date')[['cmf', 'vol_spike']].first().reset_index()

        # Merge C1 OHLCV + indicators
        c1_df = pd.merge(c1_ohlcv, c1_indicators, on='date', how='left')

        # Safety net — guarantee columns always exist
        if 'cmf' not in c1_df.columns:
            c1_df['cmf'] = 0.0
        if 'vol_spike' not in c1_df.columns:
            c1_df['vol_spike'] = 1.0

        # Final fillna for any residual NaNs
        c1_df['cmf'] = c1_df['cmf'].fillna(0)
        c1_df['vol_spike'] = c1_df['vol_spike'].fillna(1)

        c1_df['c1_color'] = np.where(c1_df['c1_close'] > c1_df['c1_open'], 'GREEN', 'RED')

        # Merge with daily features
        merged = pd.merge(
            c1_df,
            d_df[['date', 'prev_close', 'atr', 'avg_turnover_20']],
            on='date', how='inner'
        )
        if merged.empty:
            return None

        merged['symbol'] = symbol
        merged['c1_ret_pct'] = ((merged['c1_close'] - merged['prev_close']) / merged['prev_close']) * 100
        merged['atr_pct'] = (merged['atr'] / merged['prev_close']) * 100
        merged['c1_range_pct'] = ((merged['c1_high'] - merged['c1_low']) / merged['prev_close']) * 100
        merged['abs_gap_pct'] = ((merged['c1_open'] - merged['prev_close']) / merged['prev_close']).abs() * 100

        return merged

    def _rank_stocks(self, df):
        if df.empty:
            return df
        df = df.copy()
        df["is_short"] = (df["c1_color"] == "RED").astype(int)
        df["dir_cmf"] = df.apply(lambda r: r["cmf"] * (-1 if r["is_short"] else 1), axis=1)
        df["r_c1"] = df["c1_range_pct"].rank(ascending=False, method="min")
        df["r_cmf"] = df["dir_cmf"].rank(ascending=False, method="min")
        df["r_vol"] = df["vol_spike"].rank(ascending=False, method="min")
        df["r_gap"] = df["abs_gap_pct"].rank(ascending=True, method="min")
        df["final_score"] = (
            df["r_c1"] * W_C1_RANGE +
            df["r_cmf"] * W_CMF +
            df["r_vol"] * W_VOL_SPIKE +
            df["r_gap"] * W_GAP
        )
        return df.sort_values(by=["final_score"], ascending=[True]).copy()

    def _calculate_fixed_c1_stop_loss(self, direction, c1_high, c1_low):
        c1_range = c1_high - c1_low
        buffer = c1_range * SL_C1_RANGE_PCT
        return c1_low + buffer if direction == "LONG" else c1_high - buffer

    def _simulate_trade(self, entry, sl, direction, qty, breakout_time, atr, intra_day):
        trade_df = intra_day[intra_day["datetime"] >= breakout_time].copy()
        risk = abs(entry - sl)
        target = entry + (PROFIT_TRIGGER_RR * risk) if direction == "LONG" else entry - (PROFIT_TRIGGER_RR * risk)
        tsl, tsl_active = sl, False
        exit_reason, exit_p, exit_t = "EOD_EXIT", entry, breakout_time

        tick_size = 5.0 if entry >= 15000 else (0.10 if entry >= 500 else 0.05)
        entry = round(round(entry / tick_size) * tick_size, 2)
        tsl = round(round(tsl / tick_size) * tick_size, 2)

        for _, candle in trade_df.iterrows():
            exit_t = candle["datetime"]
            is_long = (direction == "LONG")

            if (is_long and candle["low"] <= tsl) or (not is_long and candle["high"] >= tsl):
                exit_reason = "STOP_LOSS" if not tsl_active else "TRAIL_SL_HIT"
                exit_p = tsl
                break

            if not tsl_active and (
                (is_long and candle["high"] >= target) or
                (not is_long and candle["low"] <= target)
            ):
                tsl_active = True

            if tsl_active:
                new_tsl = (
                    candle["close"] - (atr * TSL_ATR_MULTIPLIER) if is_long
                    else candle["close"] + (atr * TSL_ATR_MULTIPLIER)
                )
                new_tsl = round(round(new_tsl / tick_size) * tick_size, 2)
                tsl = max(tsl, new_tsl) if is_long else min(tsl, new_tsl)

            if candle["datetime"].time() >= FORCE_EXIT_TIME:
                exit_reason, exit_p = "FORCE_EXIT", candle["close"]
                break

        if exit_reason == "EOD_EXIT":
            exit_p = trade_df.iloc[-1]["close"] if not trade_df.empty else entry

        pnl = (exit_p - entry) * qty if direction == "LONG" else (entry - exit_p) * qty
        return exit_p, exit_reason, exit_t, pnl

    def run(self):
        if self.master_features.empty:
            print("❌ No features available. Exiting.")
            return

        # FIX: Use pd.Timestamp for date iteration — matches the index dtype
        curr_date = pd.to_datetime(START_DATE)
        end_date = pd.to_datetime(END_DATE)

        while curr_date <= end_date:
            if curr_date.weekday() >= 5:
                curr_date += timedelta(days=1)
                continue

            try:
                features_df = self.master_features.loc[[curr_date]].copy()
            except KeyError:
                print(f"⚠️ No data found for {curr_date.date()} — market holiday or no candles returned.")
                curr_date += timedelta(days=1)
                continue

            if features_df.empty:
                curr_date += timedelta(days=1)
                continue

            filtered_df = features_df[
                (features_df["atr_pct"] >= MIN_ATR_PCT) &
                (features_df["c1_range_pct"] >= MIN_C1_RANGE_PCT) &
                (features_df["c1_range_pct"] <= MAX_C1_RANGE_PCT) &
                (features_df["prev_close"] > MIN_PREV_DAY_CLOSE) &
                (features_df["avg_turnover_20"] > MIN_AVG_TURNOVER)
            ].copy()

            print(f"\n📅 {curr_date.date()} — {len(features_df)} stocks in features, {len(filtered_df)} passed filters")

            if filtered_df.empty:
                curr_date += timedelta(days=1)
                continue

            shortlist = pd.concat([
                filtered_df.nlargest(TOP_GAINERS_TO_SCAN, "c1_ret_pct"),
                filtered_df.nsmallest(TOP_LOSERS_TO_SCAN, "c1_ret_pct")
            ]).drop_duplicates(subset=["symbol"])

            ranked = self._rank_stocks(shortlist)
            top_stocks = ranked.head(TOP_N_STOCKS_TO_TRADE)

            print(f"   🎯 Top pick: {top_stocks.iloc[0]['symbol']} | Score: {top_stocks.iloc[0]['final_score']:.2f} | CMF: {top_stocks.iloc[0]['cmf']:.4f} | VolSpike: {top_stocks.iloc[0]['vol_spike']:.4f}")

            for _, row in top_stocks.iterrows():
                symbol = row["symbol"]
                direction = "LONG" if row["c1_color"] == "GREEN" else "SHORT"

                # FIX: Filter intraday data using pd.Timestamp date comparison
                intra_day = self.all_intra[symbol][
                    self.all_intra[symbol]["datetime"].dt.normalize() == curr_date
                ].copy()

                if intra_day.empty:
                    print(f"   ⚠️ No intraday data for {symbol} on {curr_date.date()}")
                    continue

                trig_h = row["c1_high"] * (1 + ENTRY_BUFFER_PCT / 100)
                trig_l = row["c1_low"] * (1 - ENTRY_BUFFER_PCT / 100)

                breakout_df = intra_day[
                    (intra_day["datetime"].dt.time >= BREAKOUT_LOOKBACK_START) &
                    (intra_day["datetime"].dt.time <= MAX_ENTRY_TIME)
                ]

                match = next((
                    c for _, c in breakout_df.iterrows()
                    if (direction == "LONG" and c["high"] >= trig_h) or
                       (direction == "SHORT" and c["low"] <= trig_l)
                ), None)

                if match is None:
                    print(f"   ❌ {symbol}: No breakout triggered between 09:30–10:00")
                    continue

                entry_p = trig_h if direction == "LONG" else trig_l
                sl_p = self._calculate_fixed_c1_stop_loss(direction, row["c1_high"], row["c1_low"])
                qty = int(TOTAL_CAPITAL_PER_TRADE / entry_p)

                exit_p, reason, exit_t, pnl = self._simulate_trade(
                    entry_p, sl_p, direction, qty, match["datetime"], row["atr"], intra_day
                )

                result = "WIN" if pnl > 0 else "LOSS"
                r_multiple = (
                    (exit_p - entry_p) / abs(entry_p - sl_p) if direction == "LONG"
                    else (entry_p - exit_p) / abs(entry_p - sl_p)
                )

                print(f"   📊 {symbol} | {direction} | Entry: {entry_p:.2f} | SL: {sl_p:.2f} | Exit: {exit_p:.2f} | PnL: ₹{pnl:,.0f} | {result} ({reason}) | R: {r_multiple:.2f}")

                self.trades.append({
                    "date": curr_date.date(),
                    "symbol": symbol,
                    "direction": direction,
                    "pnl": pnl,
                    "result": result,
                    "exit_reason": reason,
                    "entry_time": match["datetime"],
                    "exit_time": exit_t,
                    "entry_price": entry_p,
                    "exit_price": exit_p,
                    "sl_price": sl_p,
                    "qty": qty,
                    "r_multiple": r_multiple,
                    "cmf": row["cmf"],
                    "vol_spike": row["vol_spike"],
                    "final_score": row["final_score"],
                    "c1_range_pct": row["c1_range_pct"],
                    "atr_pct": row["atr_pct"],
                })

            curr_date += timedelta(days=1)

        self.save_and_summarize()

    def save_and_summarize(self):
        if not self.trades:
            print("\n⚠️ No trades found in the specified range.")
            print("   Possible reasons:")
            print("   1. No stock passed ATR/range/turnover filters for this date")
            print("   2. Top pick had no breakout between 09:30–10:00")
            print("   3. Market was closed / holiday")
            return

        df = pd.DataFrame(self.trades)
        df['date'] = pd.to_datetime(df['date'])
        df['month'] = df['date'].dt.to_period('M')

        monthly = df.groupby('month').agg(
            trades=('pnl', 'count'),
            wins=('result', lambda x: (x == 'WIN').sum()),
            net_pnl=('pnl', 'sum'),
            avg_r=('r_multiple', 'mean'),
            max_loss=('pnl', 'min')
        ).reset_index()
        monthly['win_rate'] = (monthly['wins'] / monthly['trades']) * 100

        print("\n" + "=" * 85)
        print(f"{'MONTH':<10} | {'TRADES':<6} | {'WIN%':<6} | {'NET PNL':<12} | {'AVG R':<6} | {'MAX LOSS':<10}")
        print("-" * 85)
        for _, row in monthly.iterrows():
            print(f"{str(row['month']):<10} | {int(row['trades']):<6} | {row['win_rate']:>5.1f}% | ₹{row['net_pnl']:>10,.0f} | {row['avg_r']:>5.2f} | ₹{row['max_loss']:>8,.0f}")
        print("-" * 85)
        print(f"{'OVERALL':<10} | {len(df):<6} | {(df['result'] == 'WIN').mean() * 100:>5.1f}% | ₹{df['pnl'].sum():>10,.0f} | {df['r_multiple'].mean():>5.2f} | {'-':<10}")
        print("=" * 85)

        for col in ['entry_time', 'exit_time']:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        df.drop(columns=['month']).to_excel(OUTPUT_FILE, index=False)
        print(f"\n✅ Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    FyersAPIBacktester().run()
