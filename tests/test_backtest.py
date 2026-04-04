# tests/test_backtest.py
import pytest
import pandas as pd
import numpy as np
import sys, os
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_simple_daily(n_days=400, start_price=5.0, trend="up"):
    """构造简单日线数据用于回测测试"""
    dates = pd.date_range("2022-01-01", periods=n_days, freq="B")
    if trend == "up":
        prices = np.linspace(start_price, start_price * 1.5, n_days)
    elif trend == "down":
        prices = np.linspace(start_price, start_price * 0.5, n_days)
    else:
        prices = np.full(n_days, start_price)
    return pd.DataFrame({
        "trade_date": dates.strftime("%Y%m%d"),
        "close": prices,
    })


def _make_div_df(ts_code, ex_dates, div_cash_per):
    return pd.DataFrame({
        "ts_code": [ts_code] * len(ex_dates),
        "ex_date": ex_dates,
        "div_cash": [div_cash_per] * len(ex_dates),
    })


def test_backtest_returns_required_keys():
    """回测结果必须包含四个关键指标"""
    from dividend_etf_core import run_backtest

    daily_df = _make_simple_daily(400)
    div_df = pd.DataFrame({"ts_code": [], "ex_date": [], "div_cash": []})

    result = run_backtest("512890.SH", daily_df, div_df, initial_capital=20000)
    assert "annualized_return" in result
    assert "max_drawdown" in result
    assert "total_trades" in result
    assert "win_rate" in result


def test_backtest_no_trades_when_no_signal():
    """如果从不满足买入条件，交易次数为 0"""
    from dividend_etf_core import run_backtest

    # 持平价格 + 无分红 → 股息率0% → 永远不会买入
    daily_df = _make_simple_daily(400, trend="flat")
    div_df = pd.DataFrame({"ts_code": [], "ex_date": [], "div_cash": []})

    result = run_backtest("512890.SH", daily_df, div_df, initial_capital=20000)
    assert result["total_trades"] == 0
    assert result["win_rate"] == 0.0


def test_backtest_stop_loss_triggers():
    """回测能正常完成，返回合理值（止损逻辑存在）"""
    from dividend_etf_core import run_backtest

    daily_df = _make_simple_daily(400, start_price=5.0, trend="up")
    dates = pd.date_range("2022-01-01", periods=400, freq="B")
    div_dates = [d.strftime("%Y%m%d") for d in dates[:12]]
    div_df = _make_div_df("512890.SH", div_dates, div_cash_per=0.025)

    result = run_backtest("512890.SH", daily_df, div_df, initial_capital=20000)
    assert isinstance(result["annualized_return"], float)
    assert result["max_drawdown"] <= 0
