"""
飞书推送服务
负责格式化每日量化报告并通过 Webhook 推送
"""
import logging
import requests

logger = logging.getLogger(__name__)

_SEP = "=" * 50


def format_report(
    date_str: str,
    buy_list: list,
    hold_list: list,
    sell_list: list,
    total_count: int,
) -> str:
    """
    将当日信号数据格式化为飞书纯文本报告。
    """
    lines = [
        "【红利ETF 每日量化报告】",
        f"日期：{date_str}",
        "策略：均衡轮动｜回测区间：近8年",
        "数据来源：Tushare Pro（反向代理）",
        "运行环境：GitHub Actions 自动执行",
        _SEP,
        "✅ 今日买入标的",
        _SEP,
    ]

    if buy_list:
        for idx, etf in enumerate(buy_list, 1):
            bt = etf.get("backtest", {})
            lines += [
                f"{idx}. 代码：{etf['ts_code']}  名称：{etf['name']}",
                f"   当前股息率：{etf['yield_pct']:.2f}%  周线RSI(14)：{etf['rsi']:.1f}",
                "   8年回测：",
                f"   • 年化收益：{bt.get('annualized_return', 0):.1f}%",
                f"   • 最大回撤：{bt.get('max_drawdown', 0):.1f}%",
                f"   • 交易次数：{bt.get('total_trades', 0)} 次",
                f"   • 胜率：{bt.get('win_rate', 0):.1f}%",
            ]
    else:
        lines.append("无")

    lines += [_SEP, "⚠️ 今日继续持有", _SEP]
    if hold_list:
        for idx, etf in enumerate(hold_list, 1):
            lines += [
                f"{idx}. 代码：{etf['ts_code']}  名称：{etf['name']}",
                f"   当前股息率：{etf['yield_pct']:.2f}%  周线RSI(14)：{etf['rsi']:.1f}",
            ]
    else:
        lines.append("无")

    lines += [_SEP, "❌ 今日卖出信号", _SEP]
    if sell_list:
        reason_map = {
            "SELL_RSI": "RSI超买",
            "SELL_YIELD": "股息率过低",
            "STOP_LOSS": "触发止损",
        }
        for idx, etf in enumerate(sell_list, 1):
            reason_cn = reason_map.get(etf.get("signal", ""), etf.get("signal", ""))
            lines.append(f"{idx}. {etf['ts_code']} {etf['name']}  原因：{reason_cn}")
    else:
        lines.append("无")

    lines += [
        _SEP,
        "📌 策略规则（自动执行）",
        _SEP,
        "买入条件：",
        "• 周线RSI(14) ≤ 40",
        "• 近12个月股息率 ≥ 4.0%",
        "卖出条件（满足任意一项）：",
        "• 周线RSI(14) ≥ 70",
        "• 近12个月股息率 ≤ 3.0%",
        "• 价格低于买入成本 -15%（止损，需用户自行监控）",
        _SEP,
        f"本次扫描覆盖全市场红利ETF共 {total_count} 只",
    ]

    return "\n".join(lines)


def push_to_feishu(webhook_url: str, message: str) -> None:
    """
    将文本消息通过飞书 Webhook 推送。
    推送失败只记录日志，不抛出异常。
    """
    payload = {
        "msg_type": "text",
        "content": {"text": message},
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        result = resp.json()
        if result.get("code") != 0:
            logger.error(f"飞书推送失败：{result}")
        else:
            logger.info("飞书推送成功")
    except Exception as e:
        logger.error(f"飞书推送异常：{e}")
