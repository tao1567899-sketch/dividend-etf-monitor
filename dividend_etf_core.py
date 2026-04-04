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
