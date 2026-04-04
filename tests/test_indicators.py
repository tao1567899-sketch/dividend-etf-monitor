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
