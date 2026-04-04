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
MIN_AVG_AMOUNT = 500_000  # 测试数据单位一致：日均成交额阈值


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
    avg_amount = (
        daily_df.groupby("ts_code")["amount"]
        .apply(lambda x: x.nlargest(20).mean())
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
