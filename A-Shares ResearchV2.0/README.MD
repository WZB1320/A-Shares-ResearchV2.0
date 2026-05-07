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

---

## 一、运行环境

- Python 版本：3.10 ~ 3.13（推荐 3.11/3.12）
- 支持系统：Windows / macOS / Linux

## 二、依赖安装

```bash
pip install -r requirements.txt
```

## 三、目录结构

```
A-Shares-ResearchV3.0-Harness/
│
├── main.py                 # 核心入口（3种运行模式）
├── app.py                  # FastAPI Web服务后端
├── .env                    # 环境变量配置（API Key/模型地址）
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
   │   ├── __init__.py
   │   ├── tech_skill.py      # 技术面：多指标系统（MA趋势/MACD/KDJ/布林带/成交量结构/趋势强度分级）
   │   ├── fund_skill.py      # 基本面：杜邦分析体系（ROE拆解/盈利质量/现金流质量/营运效率/成长持续性）
   │   ├── capital_skill.py   # 资金面：多维度资金监控（北向资金/主力资金/融资融券/龙虎榜机构动向）
   │   ├── industry_skill.py  # 行业面：产业链分析（行业周期/上下游景气度/竞争格局/政策环境）
   │   ├── valuation_skill.py # 估值面：多维估值体系（绝对估值/历史分位/相对估值/DCF模型/股息率）
   │   └── risk_skill.py      # 风险面：全面风险扫描（质押风险/减持风险/财务风险/经营风险/市场风险/治理风险）
│   │
│   ├── connectors/       # Connector 层：唯一数据入口
│   │   ├── __init__.py
│   │   ├── data_connector.py  # 统一数据获取（akshare封装）
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
│       └── chief_agent.py     # 首席Agent（整合报告）
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
│   └── index.html
│
├── test_architecture.py     # 架构测试
└── README.MD
```

## 四、使用教程

### 方式一：Web网页端（推荐）

1. 进入项目根目录
2. 启动服务：
   ```bash
   uvicorn app:app --reload
   ```
   看到 `Uvicorn running on http://127.0.0.1:8000` 即成功
3. 浏览器访问 `http://127.0.0.1:8000`
4. 输入股票代码（如 `000001`），点击「生成策略」

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

## 五、配置说明

### 大模型配置

在 `.env` 中配置 API Key，支持：
- DeepSeek
- 通义千问 (Qwen)
- 文心一言 (ERNIE)
- 豆包 (Doubao)
- 腾讯混元 (Hunyuan)
- 讯飞星火 (Spark)

### 数据源

默认使用 AkShare 免费数据源，已在 `layers/connectors/data_connector.py` 统一管理。

## 六、Agent分析维度说明

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

## 七、风险与免责

本项目仅作 **个人学习、技术研究与架构实践** 使用，不构成任何投资建议。

金融数据来源于第三方公开接口，数据准确性、完整性以官方专业数据源为准。

AI 生成的投研分析内容基于算法与公开数据，不代表投资意见，股市有风险，投资需谨慎。

所有交易决策与盈亏由使用者自行承担。

## 八、作者权益与版权声明

作者：wangzongbao
