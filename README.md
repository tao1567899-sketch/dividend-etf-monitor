# 红利ETF全自动量化监控系统

每日自动扫描A股全市场红利ETF，生成量化交易信号与8年历史回测数据，通过飞书推送专业报告。

## 部署步骤

1. Fork 或克隆本仓库到你的 GitHub 账号
2. 进入仓库 **Settings → Secrets and variables → Actions**，添加以下 3 个 Secret：

| Secret 名称 | 说明 |
|-------------|------|
| `TUSHARE_TOKEN` | Tushare Pro 个人令牌 |
| `TUSHARE_API_URL` | Tushare 反向代理接口地址 |
| `FEISHU_WEBHOOK_URL` | 飞书机器人 Webhook 地址 |

3. 进入 **Actions** 标签页，启用 Workflows
4. 每日北京时间 09:30 自动运行；也可点击 **Run workflow** 手动触发

## 策略规则

| 信号 | 条件 |
|------|------|
| 买入 | 周线RSI(14) ≤ 40 **且** 近12个月股息率 ≥ 4.0% |
| 卖出 | 周线RSI(14) ≥ 70 **或** 股息率 ≤ 3.0% |
| 止损 | 价格低于买入成本 -15%（需用户自行监控，系统不追踪持仓） |

## 项目文件说明

```
dividend-etf-monitor/
├── .github/workflows/daily_auto_run.yml  # 定时任务配置
├── dividend_etf_core.py                  # 主程序
├── feishu_push_service.py                # 飞书推送
├── requirements.txt                      # 依赖
└── README.md                             # 本文件
```

## 本地测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/ -v
```
