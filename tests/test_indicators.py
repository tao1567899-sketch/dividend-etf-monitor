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
