# tests/test_indicators.py
import pytest
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_load_config_missing_token(monkeypatch):
    """配置缺失 TUSHARE_TOKEN 时应抛出 ValueError"""
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TUSHARE_API_URL", raising=False)
    monkeypatch.delenv("FEISHU_WEBHOOK_URL", raising=False)
    from dividend_etf_core import load_config
    with pytest.raises(ValueError, match="TUSHARE_TOKEN"):
        load_config()


def test_load_config_success(monkeypatch):
    """三个配置均存在时应返回正确字典"""
    monkeypatch.setenv("TUSHARE_TOKEN", "test_token")
    monkeypatch.setenv("TUSHARE_API_URL", "http://proxy.example.com")
    monkeypatch.setenv("FEISHU_WEBHOOK_URL", "http://feishu.example.com/hook")
    from dividend_etf_core import load_config
    cfg = load_config()
    assert cfg["token"] == "test_token"
    assert cfg["api_url"] == "http://proxy.example.com"
    assert cfg["feishu_url"] == "http://feishu.example.com/hook"


def test_is_trade_day_true(mocker):
    """当日在交易日历中标记为交易日时返回 True"""
    mock_df = pd.DataFrame({
        "cal_date": ["20260404"],
        "is_open": [1]
    })
    mocker.patch("dividend_etf_core.tushare_call", return_value=mock_df)
    from dividend_etf_core import is_trade_day
    assert is_trade_day("20260404", config={"token": "t", "api_url": "u", "feishu_url": "f"}) is True


def test_is_trade_day_false(mocker):
    """当日在交易日历中标记为非交易日时返回 False"""
    mock_df = pd.DataFrame({
        "cal_date": ["20260405"],
        "is_open": [0]
    })
    mocker.patch("dividend_etf_core.tushare_call", return_value=mock_df)
    from dividend_etf_core import is_trade_day
    assert is_trade_day("20260405", config={"token": "t", "api_url": "u", "feishu_url": "f"}) is False


def test_calculate_weekly_rsi_known_value():
    """
    持续上涨序列 RSI 应 > 90；持续下跌序列 RSI 应 < 10。
    """
    from dividend_etf_core import calculate_weekly_rsi

    # 持续上涨：RSI 应接近 100
    dates = pd.date_range("2020-01-01", periods=200, freq="B")
    prices = pd.Series(range(1, 201), index=dates, dtype=float)
    df_up = pd.DataFrame({"trade_date": dates.strftime("%Y%m%d"), "close": prices.values})
    rsi_up = calculate_weekly_rsi(df_up)
    assert rsi_up is not None
    assert rsi_up > 90, f"持续上涨序列 RSI 应>90，实际={rsi_up:.2f}"

    # 持续下跌：RSI 应接近 0
    prices_down = pd.Series(range(200, 0, -1), index=dates, dtype=float)
    df_down = pd.DataFrame({"trade_date": dates.strftime("%Y%m%d"), "close": prices_down.values})
    rsi_down = calculate_weekly_rsi(df_down)
    assert rsi_down is not None
    assert rsi_down < 10, f"持续下跌序列 RSI 应<10，实际={rsi_down:.2f}"


def test_calculate_weekly_rsi_insufficient_data():
    """数据不足14周时返回 None"""
    from dividend_etf_core import calculate_weekly_rsi

    dates = pd.date_range("2024-01-01", periods=30, freq="B")
    prices = pd.Series(range(1, 31), index=dates, dtype=float)
    df = pd.DataFrame({"trade_date": dates.strftime("%Y%m%d"), "close": prices.values})
    assert calculate_weekly_rsi(df) is None


def test_calculate_ttm_yield_normal():
    """近12个月有两次分红，股息率应等于合计分红/当前价格"""
    from dividend_etf_core import calculate_ttm_yield

    today = datetime.today()
    six_months_ago = (today - timedelta(days=180)).strftime("%Y%m%d")
    ten_months_ago = (today - timedelta(days=300)).strftime("%Y%m%d")
    fifteen_months_ago = (today - timedelta(days=450)).strftime("%Y%m%d")

    div_df = pd.DataFrame({
        "ts_code":  ["512890.SH", "512890.SH", "512890.SH"],
        "ex_date":  [six_months_ago, ten_months_ago, fifteen_months_ago],
        "div_cash": [0.12, 0.10, 0.08],  # 15个月前的不应计入
    })

    # 当前价格 5.50，12个月内分红 0.12+0.10=0.22
    # 股息率 = 0.22/5.50 * 100 = 4.0%
    yield_pct = calculate_ttm_yield("512890.SH", current_price=5.50, div_df=div_df)
    assert abs(yield_pct - 4.0) < 0.01, f"期望股息率4.0%，实际{yield_pct:.4f}%"


def test_calculate_ttm_yield_no_dividend():
    """近12个月无分红时股息率为 0"""
    from dividend_etf_core import calculate_ttm_yield

    div_df = pd.DataFrame({"ts_code": [], "ex_date": [], "div_cash": []})
    assert calculate_ttm_yield("512890.SH", current_price=5.0, div_df=div_df) == 0.0


def test_calculate_ttm_yield_zero_price():
    """价格为0时返回 0，不报错"""
    from dividend_etf_core import calculate_ttm_yield

    today = datetime.today()
    div_df = pd.DataFrame({
        "ts_code": ["512890.SH"],
        "ex_date": [(today - timedelta(days=30)).strftime("%Y%m%d")],
        "div_cash": [0.10],
    })
    assert calculate_ttm_yield("512890.SH", current_price=0.0, div_df=div_df) == 0.0
