# tests/test_feishu.py
import pytest
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _sample_etf_result(signal="BUY"):
    return {
        "ts_code": "512890.SH",
        "name": "红利ETF",
        "rsi": 37.5,
        "yield_pct": 4.32,
        "signal": signal,
        "backtest": {
            "annualized_return": 12.5,
            "max_drawdown": -8.3,
            "total_trades": 6,
            "win_rate": 83.3,
        },
    }


def test_format_report_contains_date():
    """报告应包含今日日期"""
    from feishu_push_service import format_report
    report = format_report(
        date_str="2026-04-04",
        buy_list=[_sample_etf_result("BUY")],
        hold_list=[],
        sell_list=[],
        total_count=15,
    )
    assert "2026-04-04" in report


def test_format_report_buy_section():
    """买入标的出现在报告的买入区域"""
    from feishu_push_service import format_report
    report = format_report(
        date_str="2026-04-04",
        buy_list=[_sample_etf_result("BUY")],
        hold_list=[],
        sell_list=[],
        total_count=15,
    )
    assert "512890.SH" in report
    assert "红利ETF" in report
    assert "4.32" in report
    assert "37.5" in report
    assert "12.5" in report  # 年化收益


def test_format_report_empty_sections_show_none():
    """无买入/持有/卖出时对应区域显示 无"""
    from feishu_push_service import format_report
    report = format_report(
        date_str="2026-04-04",
        buy_list=[],
        hold_list=[],
        sell_list=[],
        total_count=10,
    )
    assert "无" in report


def test_push_to_feishu_calls_webhook(mocker):
    """push_to_feishu 应调用一次 requests.post"""
    mock_post = mocker.patch("feishu_push_service.requests.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"code": 0}

    from feishu_push_service import push_to_feishu
    push_to_feishu("http://feishu.example.com/hook", "测试消息")
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert "测试消息" in str(call_kwargs)
