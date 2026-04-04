"""
红利ETF全自动量化监控系统
主程序：数据获取、指标计算、信号生成、8年历史回测
"""
import os
import sys
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 配置加载
# ─────────────────────────────────────────────

def load_config() -> dict:
    """从环境变量加载配置，缺失时抛出 ValueError"""
    token = os.environ.get("TUSHARE_TOKEN")
    api_url = os.environ.get("TUSHARE_API_URL")
    feishu_url = os.environ.get("FEISHU_WEBHOOK_URL")

    if not token:
        raise ValueError("环境变量 TUSHARE_TOKEN 未设置")
    if not api_url:
        raise ValueError("环境变量 TUSHARE_API_URL 未设置")
    if not feishu_url:
        raise ValueError("环境变量 FEISHU_WEBHOOK_URL 未设置")

    return {"token": token, "api_url": api_url, "feishu_url": feishu_url}


# ─────────────────────────────────────────────
# Tushare REST API 封装（支持反向代理）
# ─────────────────────────────────────────────

def tushare_call(api_name: str, params: dict, fields: str = "", config: dict = None) -> pd.DataFrame:
    """
    直接调用 Tushare REST API（支持反向代理 URL）。
    成功返回 DataFrame，失败抛出异常。
    """
    payload = {
        "api_name": api_name,
        "token": config["token"],
        "params": params,
        "fields": fields,
    }
    try:
        resp = requests.post(config["api_url"], json=payload, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Tushare 接口请求失败 [{api_name}]: {e}") from e

    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"Tushare 接口返回错误 [{api_name}]: {result.get('msg')}")

    data = result.get("data", {})
    items = data.get("items", [])
    fields_list = data.get("fields", [])

    if not items:
        return pd.DataFrame(columns=fields_list)

    return pd.DataFrame(items, columns=fields_list)


# ─────────────────────────────────────────────
# 交易日判断
# ─────────────────────────────────────────────

def is_trade_day(date_str: str, config: dict) -> bool:
    """
    判断指定日期是否为 A 股交易日。
    date_str 格式：'YYYYMMDD'
    """
    df = tushare_call(
        "trade_cal",
        params={"exchange": "SSE", "start_date": date_str, "end_date": date_str},
        fields="cal_date,is_open",
        config=config,
    )
    if df.empty:
        return False
    return int(df.iloc[0]["is_open"]) == 1


# ─────────────────────────────────────────────
# ETF 筛选
# ─────────────────────────────────────────────

DIVIDEND_KEYWORDS = ["红利", "股息", "高息", "红利低波", "央企红利"]
MIN_LIST_MONTHS = 6
# fund_daily amount 单位：千元。500万元 = 5000千元
MIN_AVG_AMOUNT = 5_000


def screen_dividend_etfs(config: dict) -> pd.DataFrame:
    """
    筛选全市场符合条件的红利ETF。
    返回包含 ts_code, name 列的 DataFrame。
    """
    # 1. 获取全市场 ETF 基础信息
    df = tushare_call(
        "fund_basic",
        params={"market": "E", "status": "L"},
        fields="ts_code,name,fund_type,market,list_date",
        config=config,
    )

    # 2. 关键词过滤
    keyword_mask = df["name"].apply(
        lambda n: any(kw in str(n) for kw in DIVIDEND_KEYWORDS)
    )
    df = df[keyword_mask].copy()

    # 3. 上市时间过滤（≥6个月）
    cutoff = (datetime.today() - timedelta(days=MIN_LIST_MONTHS * 30)).strftime("%Y%m%d")
    df = df[df["list_date"] <= cutoff].copy()

    if df.empty:
        logger.warning("关键词+上市时间过滤后无标的")
        return df

    # 4. 流动性过滤：近20交易日日均成交额 ≥ 阈值
    end_date = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=40)).strftime("%Y%m%d")
    codes = ",".join(df["ts_code"].tolist())

    daily_df = tushare_call(
        "fund_daily",
        params={"ts_code": codes, "start_date": start_date, "end_date": end_date},
        fields="ts_code,trade_date,amount",
        config=config,
    )

    if daily_df.empty:
        logger.warning("流动性数据获取为空，跳过流动性过滤")
        return df

    daily_df["amount"] = pd.to_numeric(daily_df["amount"], errors="coerce").fillna(0)
    daily_df = daily_df.copy()
    daily_df["trade_date"] = pd.to_datetime(daily_df["trade_date"], format="%Y%m%d")
    avg_amount = (
        daily_df.sort_values("trade_date")
        .groupby("ts_code")["amount"]
        .apply(lambda x: x.iloc[-20:].mean() if len(x) >= 20 else x.mean())
        .reset_index()
        .rename(columns={"amount": "avg_amount"})
    )

    df = df.merge(avg_amount, on="ts_code", how="left")
    df["avg_amount"] = df["avg_amount"].fillna(0)
    df = df[df["avg_amount"] >= MIN_AVG_AMOUNT].copy()

    logger.info(f"筛选后红利ETF共 {len(df)} 只")
    return df[["ts_code", "name"]].reset_index(drop=True)


# ─────────────────────────────────────────────
# 周线 RSI(14) 计算
# ─────────────────────────────────────────────

def calculate_weekly_rsi(daily_df: pd.DataFrame, period: int = 14):
    """
    从日线数据计算周线 RSI(14)。
    - daily_df 需包含 'trade_date'(YYYYMMDD str) 和 'close' 列
    - 以每周最后一个交易日收盘价构建周线序列
    - 使用 Wilder 平滑（EWM alpha=1/period）
    - 数据不足 period+1 根周线时返回 None
    """
    df = daily_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.sort_values("trade_date").set_index("trade_date")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # 日线 → 周线（每周最后交易日收盘价）
    weekly_close = df["close"].resample("W").last().dropna()
    # Drop current incomplete week: only keep weeks that ended before today
    today = pd.Timestamp.today().normalize()
    weekly_close = weekly_close[weekly_close.index < today]

    if len(weekly_close) < period + 1:
        return None

    delta = weekly_close.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    # 避免除零：avg_loss 为 0 时 RSI = 100（纯上涨）；avg_gain 为 0 时 RSI = 0（纯下跌）
    avg_loss_abs = avg_loss.abs()
    avg_gain_abs = avg_gain.abs()
    rs = np.where(avg_loss_abs == 0, np.inf, avg_gain_abs / avg_loss_abs)
    rsi = pd.Series(100 - (100 / (1 + rs)), index=avg_gain.index)

    last_rsi = rsi.iloc[-1]
    return round(float(last_rsi), 2) if not np.isnan(last_rsi) else None


# ─────────────────────────────────────────────
# TTM 股息率计算
# ─────────────────────────────────────────────

def calculate_ttm_yield(ts_code: str, current_price: float, div_df: pd.DataFrame) -> float:
    """
    计算 TTM（近12个月）股息率。
    - div_df 需包含 ts_code, ex_date(YYYYMMDD str), div_cash 列
    - 返回百分比形式（如 4.0 表示 4.0%）
    - 无分红或价格为0时返回 0.0
    """
    if current_price <= 0:
        return 0.0

    if div_df.empty:
        return 0.0

    cutoff = (datetime.today() - timedelta(days=365)).strftime("%Y%m%d")
    code_div = div_df[
        (div_df["ts_code"].astype(str) == ts_code) &
        (div_df["ex_date"].astype(str) >= cutoff)
    ]

    if code_div.empty:
        return 0.0

    total_div = pd.to_numeric(code_div["div_cash"], errors="coerce").fillna(0).sum()
    return round(total_div / current_price * 100, 4)


# ─────────────────────────────────────────────
# 交易信号生成
# ─────────────────────────────────────────────

# 策略阈值
RSI_BUY_THRESHOLD = 40.0
RSI_SELL_THRESHOLD = 70.0
YIELD_BUY_THRESHOLD = 4.0    # %
YIELD_SELL_THRESHOLD = 3.0   # %


def generate_signal(rsi: float, yield_pct: float) -> str:
    """
    根据周线 RSI 和 TTM 股息率生成交易信号。
    返回值：'BUY' | 'SELL_RSI' | 'SELL_YIELD' | 'HOLD'

    注：止损条件（买入成本×0.85）仅在回测中追踪，每日信号不含止损判断。
    """
    # 卖出条件优先（RSI 超买优先于股息率过低）
    if rsi >= RSI_SELL_THRESHOLD:
        return "SELL_RSI"
    if yield_pct <= YIELD_SELL_THRESHOLD and yield_pct > 0:
        return "SELL_YIELD"

    # 买入条件（需同时满足）
    if rsi <= RSI_BUY_THRESHOLD and yield_pct >= YIELD_BUY_THRESHOLD:
        return "BUY"

    return "HOLD"


# ─────────────────────────────────────────────
# 历史数据获取辅助函数
# ─────────────────────────────────────────────

def fetch_8year_daily(ts_code: str, config: dict) -> pd.DataFrame:
    """获取单只 ETF 近8年日线数据"""
    end_date = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=365 * 8 + 30)).strftime("%Y%m%d")
    try:
        df = tushare_call(
            "fund_daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
            fields="ts_code,trade_date,close,amount",
            config=config,
        )
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df.dropna(subset=["close"]).sort_values("trade_date").reset_index(drop=True)
    except Exception as e:
        logger.warning(f"获取 {ts_code} 日线数据失败：{e}")
        return pd.DataFrame()


def fetch_12month_div(ts_code: str, config: dict) -> pd.DataFrame:
    """获取单只 ETF 近14个月分红数据（多2个月缓冲）"""
    end_date = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=365 + 60)).strftime("%Y%m%d")
    try:
        df = tushare_call(
            "fund_div",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
            fields="ts_code,ex_date,div_cash",
            config=config,
        )
        df["div_cash"] = pd.to_numeric(df["div_cash"], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.warning(f"获取 {ts_code} 分红数据失败：{e}")
        return pd.DataFrame(columns=["ts_code", "ex_date", "div_cash"])


def fetch_8year_div(ts_code: str, config: dict) -> pd.DataFrame:
    """获取单只 ETF 近8年分红数据（供回测使用）"""
    end_date = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=365 * 8 + 30)).strftime("%Y%m%d")
    try:
        df = tushare_call(
            "fund_div",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
            fields="ts_code,ex_date,div_cash",
            config=config,
        )
        df["div_cash"] = pd.to_numeric(df["div_cash"], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.warning(f"获取 {ts_code} 8年分红数据失败：{e}")
        return pd.DataFrame(columns=["ts_code", "ex_date", "div_cash"])


# ─────────────────────────────────────────────
# 8 年历史回测
# ─────────────────────────────────────────────

STOP_LOSS_RATIO = 0.85   # 买入成本的 85%（-15% 止损）
INITIAL_CAPITAL = 20_000


def run_backtest(
    ts_code: str,
    daily_df: pd.DataFrame,
    div_df: pd.DataFrame,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict:
    """
    对单只 ETF 运行历史回测。
    - daily_df: trade_date(YYYYMMDD), close
    - div_df: ts_code, ex_date(YYYYMMDD), div_cash
    返回 dict: annualized_return, max_drawdown, total_trades, win_rate
    """
    df = daily_df.copy().sort_values("trade_date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])

    if len(df) < 100:
        return {"annualized_return": 0.0, "max_drawdown": 0.0, "total_trades": 0, "win_rate": 0.0}

    capital = float(initial_capital)
    position = 0        # 持有份数
    buy_price = 0.0
    trades = []
    portfolio_values = []

    # ─── 预计算周线 RSI 系列（避免 O(n²)）───
    df_for_rsi = df.copy()
    df_for_rsi["trade_date_dt"] = pd.to_datetime(df_for_rsi["trade_date"], format="%Y%m%d")
    df_for_rsi = df_for_rsi.set_index("trade_date_dt")
    weekly_close = df_for_rsi["close"].resample("W").last().dropna()
    today = pd.Timestamp.today().normalize()
    weekly_close = weekly_close[weekly_close.index < today]

    if len(weekly_close) >= 15:
        delta = weekly_close.diff().dropna()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss_safe = avg_loss.replace(0, np.nan)
        rs = avg_gain / avg_loss_safe
        rsi_series = (100 - (100 / (1 + rs))).dropna()
        rsi_index = rsi_series.index.tolist()
        rsi_map = {week_end: rsi_val for week_end, rsi_val in zip(rsi_index, rsi_series.values.tolist())}

        def get_rsi_for_date(trade_date_str):
            dt = pd.to_datetime(trade_date_str, format="%Y%m%d")
            past_weeks = [w for w in rsi_index if w <= dt]
            if not past_weeks:
                return None
            return rsi_map[past_weeks[-1]]
    else:
        def get_rsi_for_date(trade_date_str):
            return None

    for i in range(len(df)):
        row = df.iloc[i]
        current_price = float(row["close"])
        current_date = str(row["trade_date"])

        # 当前持仓市值
        portfolio_values.append(capital + position * current_price)

        # ─── 已持仓：检查卖出条件 ───
        if position > 0:
            stop_price = buy_price * STOP_LOSS_RATIO
            yield_pct = calculate_ttm_yield(ts_code, current_price, div_df)
            rsi = get_rsi_for_date(current_date)
            rsi_val = rsi if rsi is not None else 50.0

            sell_reason = None
            if current_price <= stop_price:
                sell_reason = "STOP_LOSS"
            elif rsi_val >= RSI_SELL_THRESHOLD:
                sell_reason = "SELL_RSI"
            elif 0 < yield_pct <= YIELD_SELL_THRESHOLD:
                sell_reason = "SELL_YIELD"

            if sell_reason:
                proceeds = position * current_price
                profit_pct = (current_price - buy_price) / buy_price * 100
                capital = proceeds
                trades.append({"date": current_date, "profit_pct": profit_pct, "reason": sell_reason})
                position = 0
                buy_price = 0.0

        # ─── 空仓：检查买入条件 ───
        elif position == 0:
            rsi = get_rsi_for_date(current_date)
            if rsi is None:
                continue
            yield_pct = calculate_ttm_yield(ts_code, current_price, div_df)

            if rsi <= RSI_BUY_THRESHOLD and yield_pct >= YIELD_BUY_THRESHOLD:
                shares = int(capital / current_price / 100) * 100  # 按100份整手买入
                if shares > 0:
                    position = shares
                    buy_price = current_price
                    capital -= shares * current_price

    # 收尾：强制平仓
    if position > 0:
        last_price = float(df.iloc[-1]["close"])
        profit_pct = (last_price - buy_price) / buy_price * 100
        capital += position * last_price
        trades.append({"date": str(df.iloc[-1]["trade_date"]), "profit_pct": profit_pct, "reason": "END"})

    # ─── 计算指标 ───
    total_return = (capital - initial_capital) / initial_capital
    start_dt = pd.to_datetime(df.iloc[0]["trade_date"], format="%Y%m%d")
    end_dt = pd.to_datetime(df.iloc[-1]["trade_date"], format="%Y%m%d")
    years = max((end_dt - start_dt).days / 365.25, 0.01)
    annualized_return = ((1 + total_return) ** (1 / years) - 1) * 100

    # 最大回撤
    pv = pd.Series(portfolio_values, dtype=float)
    rolling_max = pv.expanding().max()
    drawdowns = (pv - rolling_max) / rolling_max * 100
    max_drawdown = float(drawdowns.min())

    # 胜率（排除 END 平仓）
    closed_trades = [t for t in trades if t["reason"] != "END"]
    win_trades = [t for t in closed_trades if t["profit_pct"] > 0]
    win_rate = len(win_trades) / len(closed_trades) * 100 if closed_trades else 0.0

    return {
        "annualized_return": round(annualized_return, 2),
        "max_drawdown": round(max_drawdown, 2),
        "total_trades": len(closed_trades),
        "win_rate": round(win_rate, 2),
    }


# ─────────────────────────────────────────────
# 主程序入口
# ─────────────────────────────────────────────

def main():
    from feishu_push_service import format_report, push_to_feishu

    # 1. 加载配置
    config = load_config()
    today_str = datetime.today().strftime("%Y%m%d")

    # 2. 交易日检查
    if not is_trade_day(today_str, config):
        logger.info(f"[SKIP] 今日 {today_str} 非交易日，程序退出")
        return

    logger.info(f"[START] 今日 {today_str} 开始运行红利ETF监控")

    # 3. ETF 筛选
    etf_list = screen_dividend_etfs(config)
    if etf_list.empty:
        logger.warning("筛选后无红利ETF标的，程序退出")
        return

    total_count = len(etf_list)
    logger.info(f"筛选到红利ETF {total_count} 只，开始逐只分析...")

    buy_list, hold_list, sell_list = [], [], []

    for _, row in etf_list.iterrows():
        ts_code = row["ts_code"]
        name = row["name"]
        logger.info(f"  分析：{ts_code} {name}")

        try:
            # 4. 获取数据
            daily_df = fetch_8year_daily(ts_code, config)
            if daily_df.empty or len(daily_df) < 100:
                logger.warning(f"  {ts_code} 数据不足，跳过")
                continue

            div_df = fetch_12month_div(ts_code, config)

            # 5. 计算指标
            current_price = float(daily_df.iloc[-1]["close"])
            rsi = calculate_weekly_rsi(daily_df)
            yield_pct = calculate_ttm_yield(ts_code, current_price, div_df)

            if rsi is None:
                logger.warning(f"  {ts_code} RSI 计算失败（数据不足），跳过")
                continue

            # 6. 生成信号
            signal = generate_signal(rsi, yield_pct)

            # 7. 回测（仅买入标的计算回测）
            backtest_result = {"annualized_return": 0.0, "max_drawdown": 0.0, "total_trades": 0, "win_rate": 0.0}
            if signal == "BUY":
                backtest_div_df = fetch_8year_div(ts_code, config)
                backtest_result = run_backtest(ts_code, daily_df, backtest_div_df)

            etf_result = {
                "ts_code": ts_code,
                "name": name,
                "rsi": rsi,
                "yield_pct": yield_pct,
                "signal": signal,
                "backtest": backtest_result,
            }

            if signal == "BUY":
                buy_list.append(etf_result)
            elif signal.startswith("SELL"):
                sell_list.append(etf_result)
            else:
                hold_list.append(etf_result)

        except Exception as e:
            logger.error(f"  处理 {ts_code} 时发生异常：{e}")
            continue

    # 多只买入信号时，仅保留股息率最高者（其余降为持有）
    if len(buy_list) > 1:
        buy_list.sort(key=lambda x: x["yield_pct"], reverse=True)
        logger.info(f"多只买入信号，选择股息率最高：{buy_list[0]['ts_code']} {buy_list[0]['yield_pct']:.2f}%")
        hold_list.extend(buy_list[1:])
        buy_list = [buy_list[0]]

    # 8. 飞书推送
    date_display = datetime.today().strftime("%Y-%m-%d")
    report = format_report(date_display, buy_list, hold_list, sell_list, total_count)
    push_to_feishu(config["feishu_url"], report)

    logger.info(f"[DONE] 完成：买入{len(buy_list)}只，持有{len(hold_list)}只，卖出{len(sell_list)}只")


if __name__ == "__main__":
    main()
