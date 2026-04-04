# tests/test_signals.py
import pytest
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_signal_buy():
    """RSI<=40 AND 股息率>=4.0% → 买入"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=38.0, yield_pct=4.5) == "BUY"


def test_signal_sell_rsi():
    """RSI>=70 → 卖出"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=72.0, yield_pct=3.5) == "SELL_RSI"


def test_signal_sell_yield():
    """股息率<=3.0% AND 股息率>0 → 卖出"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=55.0, yield_pct=2.8) == "SELL_YIELD"


def test_signal_sell_rsi_takes_priority_over_yield():
    """RSI>=70 AND 股息率<=3% 时，RSI 优先"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=75.0, yield_pct=2.5) == "SELL_RSI"


def test_signal_hold():
    """不满足买入和卖出条件 → 持有"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=55.0, yield_pct=3.5) == "HOLD"


def test_signal_buy_boundary_rsi():
    """RSI 恰好等于 40 时触发买入"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=40.0, yield_pct=4.0) == "BUY"


def test_signal_no_dividend():
    """股息率为0（无分红数据）时不触发买入"""
    from dividend_etf_core import generate_signal
    assert generate_signal(rsi=35.0, yield_pct=0.0) == "HOLD"
