# AI A股多智能体智能投研报告系统

## 项目介绍

本项目基于 **Anthropic Harness 多智能体架构** 开发，采用三层架构设计（Skills/Connectors/Agents），专为A股投资研究场景构建。

### 核心架构

```
┌─────────────────────────────────────────────────────────┐
│                    Chief Agent                          │
│              (首席投研Agent - 汇总各维度分析)            │
└─────────────────────────────────────────────────────────┘
                           │
    ┌──────────────────────┼──────────────────────┐
    │                      │                      │
    ▼                      ▼                      ▼
┌─────────┐          ┌─────────┐          ┌─────────┐
│ Data    │          │ Tech    │          │ Fund    │
│ Agent   │          │ Agent   │          │ Agent   │
└─────────┘          └─────────┘          └─────────┘
    │                      │                      │
    ▼                      ▼                      ▼
┌─────────┐          ┌─────────┐          ┌─────────┐
│Capital  │          │Industry  │          │Valuation│
│ Agent   │          │ Agent    │          │ Agent   │
└─────────┘          └─────────┘          └─────────┘
    │                      │                      │
    ▼                      ▼                      ▼
┌─────────┐          ┌─────────┐          ┌─────────┐
│ Risk    │          │ (Skill层统一由各Agent调用)     │
│ Agent   │          │                              │
└─────────┘          └──────────────────────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │    Data    │
                   │  Connector │
                   └─────────────┘
```

### 三层架构职责

| 层级 | 目录 | 职责 |
|------|------|------|
| **Skills** | `layers/skills/` | 纯计算规则，负责量化指标计算与信号识别 |
| **Connectors** | `layers/connectors/` | 唯一数据入口，统一管理数据获取与工具调用 |
| **Agents** | `layers/agents/` | 纯执行层，调用Skill和Connector生成报告 |

### 核心特点

- **Harness三层架构**：职责清晰解耦，Skills层纯计算，Agents层纯执行
- **多Agent分工协作**：7个专业分析Agent（技术面/基本面/资金面/行业面/估值面/风险面）+ 数据Agent + 首席Agent
- **首席Agent整合**：Chief Agent汇聚各维度分析，生成机构级投研报告
- **断点续跑机制**：HarnessStateManager支持任务状态持久化与中断恢复
- **数据校验体系**：HarnessValidator验证数据完整性、股票代码格式、估值指标合理性
- **并行调度优化**：HarnessScheduler支持多Agent并行执行与失败重试机制
- **✨ 自选股管理**：支持添加/删除自选股、批量刷新快照
- **✨ 知识库持久化**：分析快照自动存储、历史回溯对比、核心结论追踪
- **✨ 机构研报获取**：五层筛选（白名单/深度/时效/自选股/上限），支持A股+港股+美股研报自动拉取
- **✨ 事件时间线**：分析信号与风险自动写入时间线，支持事件回溯
- **✨ ECharts可视化**：8种图表（K线/MACD/KDJ/RSI/雷达图/估值仪表/资金流向/杜邦分析）
- **✨ 上下文对话**：分析完成后可基于报告继续追问（"估值合理吗？""主要风险是什么？"）
- **✨ 三栏布局Web UI**：左侧自选股、中间分析区、右侧对话面板，支持异步AJAX分析

---

## 一、运行环境

- Python 版本：3.10 ~ 3.13（推荐 3.11/3.12）
- 支持系统：Windows / macOS / Linux

## 二、依赖安装

```bash
pip install -r requirements.txt
```

额外Web前端依赖：
```bash
pip install jinja2 python-multipart -q
```

## 三、目录结构

```
A-Shares-ResearchV3.0-Harness/
│
├── main.py                 # 核心入口（3种运行模式）
├── app.py                  # FastAPI Web服务后端
├── .env                    # 环境变量配置（API Key/模型地址）
├── db.py                   # SQLite数据库层（自选股表 + 分析缓存表）
├── chart_builder.py        # 图表数据构建器（ECharts兼容JSON）
├── chat_engine.py          # 上下文对话引擎（分析后追问）
│
├── harness/                # Anthropic Harness 核心调度层
│   ├── __init__.py
│   ├── state.py           # 白盒状态管理 + 断点续跑
│   ├── validator.py       # 结果校验（数据完整性/风险）
│   └── scheduler.py       # 并行调度器 + 重试机制
│
├── layers/                 # Anthropic 三层架构
│   ├── __init__.py
│   │
│   ├── skills/            # Skill 层：机构级纯计算规则
│   │   ├── __init__.py
│   │   ├── tech_skill.py      # 技术面：多指标系统（MA趋势/MACD/KDJ/布林带/成交量结构/趋势强度分级）
│   │   ├── fund_skill.py      # 基本面：杜邦分析体系（ROE拆解/盈利质量/现金流质量/营运效率/成长持续性）
│   │   ├── capital_skill.py   # 资金面：多维度资金监控（北向资金/主力资金/融资融券/龙虎榜机构动向）
│   │   ├── industry_skill.py  # 行业面：产业链分析（行业周期/上下游景气度/竞争格局/政策环境）
│   │   ├── valuation_skill.py # 估值面：多维估值体系（绝对估值/历史分位/相对估值/DCF模型/股息率）
│   │   └── risk_skill.py      # 风险面：全面风险扫描（质押风险/减持风险/财务风险/经营风险/市场风险/治理风险）
│   │
│   ├── connectors/       # Connector 层：唯一数据入口
│   │   ├── __init__.py
│   │   ├── data_connector.py  # 统一数据获取（多数据源：akshare + tushare，自动降级）
│   │   └── tool_connector.py  # 工具调用封装
│   │
│   └── agents/            # Agent 层：纯执行
│       ├── __init__.py
│       ├── data_agent.py      # 数据Agent（统一获取各维度数据）
│       ├── tech_agent.py      # 技术面Agent
│       ├── fund_agent.py      # 基本面Agent
│       ├── capital_agent.py   # 资金面Agent
│       ├── industry_agent.py  # 行业面Agent
│       ├── risk_agent.py      # 风险面Agent
│       ├── valuation_agent.py # 估值面Agent
│       ├── chief_agent.py     # 首席Agent（整合报告 + chart_data + context_text）
│       ├── debate.py          # 多Agent辩论机制（跨维度审阅+修订）
│       └── report_schema.py   # 结构化报告Schema（AgentReport/聚合/评分映射）
│
├── layers/memory/          # 知识库 + 研报管理
│   ├── __init__.py
│   ├── knowledge_base.py     # SQLite知识库（分析快照/研报/事件时间线）
│   └── report_fetcher.py     # 研报获取器（A股AkShare + 港股AkShare + 美股Finnhub）
│
├── graph/                  # LangGraph 工作流（兼容备用）
│   └── workflow.py
│
├── config/                 # 配置文件
│   ├── __init__.py
│   ├── env_config.py
│   └── llm_config.py
│
├── templates/               # 前端页面
│   └── index.html          # 三栏布局Web页（自选股/分析/对话）
│
├── test_architecture.py     # 架构测试
└── README.MD
```

## 四、使用教程

### 方式一：Web网页端（推荐）

1. 进入项目根目录
2. 启动投研服务：
   ```bash
   uvicorn app:app --reload --port 8080
   ```
   或者直接运行：
   ```bash
   python app.py
   ```
   看到 `Uvicorn running on http://127.0.0.1:8080` 即成功
3. 浏览器访问 `http://127.0.0.1:8080`

**✨ Web端功能说明**：
- **左侧自选股**：输入6位股票代码点击「+添加」，支持删除和批量刷新快照
- **中间分析区**：输入股票代码点击「开始分析」→ 异步AJAX分析 → 显示评分徽章 + 8种ECharts图表 + Markdown报告
- **右侧对话面板**：分析完成后自动激活，可基于报告内容继续追问（如："当前估值合理吗？"、"主要风险是什么？"）
- **知识库自动积累**：每次分析自动保存快照（评分/结论/信号/风险），刷新自选股时自动拉取机构研报，分析信号与风险自动写入事件时间线
- **两种入口**：
  - 输入框直接输入代码 → 分析单只股票
  - 点击自选股列表中的任意一项 → 快速分析

### 方式二：控制台运行

```bash
python main.py
```

可选运行模式：
- **ChiefAgent**：Harness架构，单Agent顺序执行（推荐）
- **HarnessScheduler**：Harness架构，多Agent并行调度
- **LangGraph Workflow**：兼容LangGraph架构，用于版本平滑迁移

### 方式三：架构测试

```bash
python test_architecture.py
```

## 五、API 接口说明

| 方法 | 路径 | 功能 | 请求/响应 |
|------|------|------|-----------|
| GET | `/` | Web首页 | HTML |
| POST | `/generate` | 表单提交分析（兼容旧版） | form-data: stock_code, agents |
| GET | `/api/watchlist` | 查询自选股列表 | JSON: `[{stock_code, stock_name, last_price, last_grade, ...}]` |
| POST | `/api/watchlist` | 添加自选股 | JSON: `{"stock_code": "600519"}` |
| DELETE | `/api/watchlist/{stock_code}` | 删除自选股 | JSON: `{"success": true}` |
| POST | `/api/watchlist/refresh` | 批量刷新自选股快照 + 自动拉取研报 | JSON: `{"results": [...]}` |
| POST | `/api/analyze` | AJAX异步分析股票 | JSON: `{"stock_code": "600519", "agents": ["tech", "fund", ...]}` → 返回 `{success, final_report, chart_data, overall_grade, overall_score}` |
| POST | `/api/chat` | 上下文对话追问 | JSON: `{"stock_code": "600519", "messages": [...], "new_message": "估值合理吗？"}` → 返回 `{reply}` |

## 六、配置说明

### 大模型配置

在 `.env` 中配置 API Key，支持：
- DeepSeek
- 通义千问 (Qwen)
- 文心一言 (ERNIE)
- 豆包 (Doubao)
- 腾讯混元 (Hunyuan)
- 讯飞星火 (Spark)

### 数据源

默认使用 AkShare 免费数据源，同时支持 Tushare Pro（需配置 Token）。系统自动检测可用数据源并在失败时自动降级切换，详见 `layers/connectors/data_connector.py`。

### 知识库架构

知识库基于 SQLite 持久化存储（`data/knowledge/stock_memory.db`），包含三张核心表：

| 表名 | 用途 | 写入时机 |
|------|------|----------|
| `analysis_snapshots` | 每次分析的评分/结论/信号/风险快照 | ChiefAgent 分析完成后自动写入 |
| `research_reports` | 机构券商研报（标题/机构/评级/摘要） | 刷新自选股时自动拉取 |
| `stock_timeline` | 重大信号与风险事件时间线 | ChiefAgent 分析完成后自动写入 |

**研报获取五层筛选漏斗**：
1. **机构白名单**：Top 30 券商（中金/中信/华泰等）
2. **深度报告**：标题含"深度/首次覆盖/专题"等关键词，排除"晨会/日报/点评"
3. **时效性**：发布时间 ≤ 180天
4. **自选股限定**：仅拉取自选股列表中的股票研报
5. **数量上限**：每只股票最多保留 10 篇（最新优先）

**多市场研报支持**：
- A股：AkShare 东方财富研报接口
- 港股：AkShare 港股研报接口
- 美股：Finnhub Company News API（需配置 `FINNHUB_API_KEY`）

## 七、Agent分析维度说明

| Agent | 分析维度 | 核心指标 |
|-------|----------|----------|
| DataAgent | 数据获取 | 统一获取各维度原始数据 |
| TechAgent | 技术面 | 均线系统/MACD/KDJ/布林带/成交量结构/趋势强度分级/背离识别 |
| FundAgent | 基本面 | 杜邦ROE拆解/盈利质量评估/现金流质量/营运效率/成长持续性/财务健康度 |
| CapitalAgent | 资金面 | 北向资金流向/主力资金动向/融资融券余额/龙虎榜机构席位分析 |
| IndustryAgent | 行业面 | 行业周期判断/产业链上下游景气度/竞争格局(CR3/CR5)/政策环境影响 |
| ValuationAgent | 估值面 | PE/PB/PS绝对估值/历史十年分位/行业相对估值/DCF折现模型/股息率 |
| RiskAgent | 风险面 | 股权质押风险/大股东减持/财务风险/经营集中度风险/市场波动风险/公司治理风险 |
| ChiefAgent | 整合报告 | 汇总各维度分析，生成机构级投研报告 |

## 八、风险与免责

本项目仅作 **个人学习、技术研究与架构实践** 使用，不构成任何投资建议。

金融数据来源于第三方公开接口，数据准确性、完整性以官方专业数据源为准。

AI 生成的投研分析内容基于算法与公开数据，不代表投资意见，股市有风险，投资需谨慎。

所有交易决策与盈亏由使用者自行承担。

## 九、作者权益与版权声明

作者：wangzongbao
