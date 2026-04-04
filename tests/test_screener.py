# tests/test_screener.py
import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CFG = {"token": "t", "api_url": "u", "feishu_url": "f"}


def _make_fund_basic():
    """构造 fund_basic 样本数据"""
    today = pd.Timestamp.today()
    eight_months_ago = (today - pd.DateOffset(months=8)).strftime("%Y%m%d")
    six_months_ago = (today - pd.DateOffset(months=6)).strftime("%Y%m%d")
    three_months_ago = (today - pd.DateOffset(months=3)).strftime("%Y%m%d")

    return pd.DataFrame({
        "ts_code":   ["512890.SH", "510300.SH", "512980.SH", "159905.SZ", "512810.SH"],
        "name":      ["红利ETF",    "沪深300ETF",  "消费ETF",   "红利低波ETF", "央企红利ETF"],
        "fund_type": ["ETF",       "ETF",        "ETF",      "ETF",       "ETF"],
        "market":    ["E",         "E",          "E",        "E",         "E"],
        "list_date": [eight_months_ago, eight_months_ago, eight_months_ago,
                      six_months_ago, three_months_ago],  # 最后一只上市不足6个月
    })


def _make_fund_daily_liquid():
    """20日成交额数据：512890 和 159905 满足500万，512810 不满足"""
    records = []
    for code, amt in [("512890.SH", 8000000), ("159905.SZ", 6000000), ("512810.SH", 3000000)]:
        for i in range(20):
            records.append({"ts_code": code, "trade_date": f"202604{i+1:02d}", "amount": amt / 10})
    return pd.DataFrame(records)


def test_screen_by_keyword(mocker):
    """仅含关键词的ETF通过筛选"""
    fb = _make_fund_basic()
    fd = _make_fund_daily_liquid()

    call_returns = iter([fb, fd])
    mocker.patch("dividend_etf_core.tushare_call", side_effect=lambda *a, **kw: next(call_returns))

    from dividend_etf_core import screen_dividend_etfs
    result = screen_dividend_etfs(config=CFG)

    codes = set(result["ts_code"].tolist())
    assert "512890.SH" in codes        # 红利ETF ✓
    assert "159905.SZ" in codes        # 红利低波ETF ✓
    assert "510300.SH" not in codes    # 沪深300ETF ✗（无关键词）
    assert "512980.SH" not in codes    # 消费ETF ✗（无关键词）


def test_screen_list_date_filter(mocker):
    """上市不足6个月的ETF被过滤"""
    fb = _make_fund_basic()
    fd = _make_fund_daily_liquid()

    call_returns = iter([fb, fd])
    mocker.patch("dividend_etf_core.tushare_call", side_effect=lambda *a, **kw: next(call_returns))

    from dividend_etf_core import screen_dividend_etfs
    result = screen_dividend_etfs(config=CFG)

    assert "512810.SH" not in result["ts_code"].tolist()  # 上市不足6个月 ✗


def test_screen_liquidity_filter(mocker):
    """近20日日均成交额 < 500万的ETF被过滤"""
    fb = _make_fund_basic()
    fd = _make_fund_daily_liquid()

    call_returns = iter([fb, fd])
    mocker.patch("dividend_etf_core.tushare_call", side_effect=lambda *a, **kw: next(call_returns))

    from dividend_etf_core import screen_dividend_etfs
    result = screen_dividend_etfs(config=CFG)

    # 159905 因流动性达标而保留
    assert "159905.SZ" in result["ts_code"].tolist()
