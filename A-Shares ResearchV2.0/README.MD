\# AI A股多Agent 智能投研报告系统



\## 项目介绍

本项目基于 LangGraph 多智能体架构开发，整合数据采集、技术面分析、基本面财报解析、十年估值分位研判、个股风险筛查、首席投研整合总结全链路。

输入A股证券代码，自动生成标准化、机构级深度投研结论报告，支持 Web 网页端可视化使用。



核心特点：

\- 多Agent分工协作，分析维度全面

\- 自动识别异常数据、财务暴雷、退市高风险标的

\- 包含同业对比、历史趋势、边际变化、多情景假设

\- 输出专业评级、明确操作建议，逻辑闭环

\- 本地轻量化部署，纯离线个人使用



\---



\## 一、运行环境

\- Python 版本：3.10 \~ 3.12 推荐

\- 支持系统：Windows / macOS / Linux



\## 二、依赖安装

打开终端，执行以下命令安装全部依赖：

```bash

pip install akshare langchain langchain-openai fastapi uvicorn jinja2


\## 三、目录结构

MaxAgenttrading/

├── main.py               # 核心入口、LangGraph流程、全部Agent逻辑

├── app.py                # Web网页服务后端

├── .env                  # 环境变量配置文件（密钥/模型地址）

├── agents/               # 多智能体核心模块（独立分工）
│   ├── __init__.py
│   ├── data_fetcher.py       # 统一数据获取Agent（全系统唯一数据入口）
│   ├── tech_analyzer.py      # 技术面分析Agent
│   ├── fund_analyzer.py      # 基本面分析Agent
│   ├── capital_analyzer.py   # 资金面分析Agent
│   ├── industry_analyzer.py  # 行业面分析Agent
│   ├── risk_analyzer.py      # 风险筛查Agent
│   ├── valuation_analyzer.py # 估值分析Agent（PE/PB十年分位）
│   └── chief_reviewer.py     # 首席投研总监Agent（最终整合决策）

├── graph/                # LangGraph 工作流编排
│   ├── __init__.py
│   └── workflow.py      # 流程调度、节点连接、状态管理
├── README.md             # 项目说明、安装教程、版权声明

├── tools/                # 公共工具目录

│   ├── \_\_init\_\_.py

│   ├── data\_tool.py      # 股票金融数据获取工具

│   └── utils.py          # 通用计算、辅助工具函数

└── templates/            # 前端页面模板

&#x20;   └── index.html        # 浏览器交互首页



\## 四、使用教程



方式一：Web 网页端（推荐）

1\.进入项目根目录

2\.启动网页服务：

bash

uvicorn app:app --reload

看到下面这句话就代表成功了：

Uvicorn running on http://127.0.0.1:8000

3\.浏览器访问本地地址：

plaintext

http://127.0.0.1:8000

输入 A 股股票代码，点击「生成投研报告」

自动全流程分析，页面展示完整格式化投研报告



方式二：控制台终端运行

直接运行 main.py

在命令行输入股票代码，终端直接输出完整投研报告。

方式三：LangGraph 工作流模式
启动命令python -m graph.workflow

注意事项：
1.数据源配置：默认使用免费 akshare 数据源，可在 agents/data_fetcher.py 中替换为 Tushare / Wind / 同花顺等专业数据源

2.大模型配置：.env中需要配置自己的大模型API key；

3.模型切换：在 config/llm_config.py 中修改模型名称与接口配置，支持 DeepSeek / 通义千问 / 豆包等主流大模型一键切换



\## 五、风险与免责

本项目仅作 个人学习、技术研究与架构实践 使用，不构成任何投资建议。

金融数据来源于第三方公开接口，数据准确性、完整性以官方专业数据源为准。

AI 生成的投研分析内容基于算法与公开数据，不代表投资意见，股市有风险，投资需谨慎。

所有交易决策与盈亏由使用者自行承担。



\##六、作者权益与版权声明

作者：wangzongbao

本项目为个人独立原创开发，所有代码、提示词设计、架构逻辑均为原创。

仅限非商业、个人学习使用，禁止未经授权二次售卖、封装付费、商用盈利。

禁止删除、篡改本项目原创标识与作者版权声明。

二次修改、转发分享时，必须保留原作者信息。

本项目保留全部合法修改权、解释权与后续更新版权。





