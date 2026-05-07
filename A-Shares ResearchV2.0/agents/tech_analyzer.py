import sys
import logging
from pathlib import Path
from typing import Optional, List, Dict

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("技术面Agent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from agents.data_fetcher import DataFetcherAgent
import pandas as pd

REPORT_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.5

class TechAnalyzerAgent:
    """技术面分析Agent类（支持多模型/全量技术指标/可视化）"""
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_fetcher = data_fetcher

    def analyze_tech(self, stock_code: str, tech_data: Optional[List[Dict]] = None) -> str:
        """生成机构级技术面分析报告"""
        if tech_data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            df = self.data_fetcher.get_tech_data()
        else:
            df = pd.DataFrame(tech_data)
        
        latest = df.iloc[-1]
        recent_data = df.tail(30).to_dict("records")
        data_str = f"最新数据点：{latest.to_dict()}\n近30天数据摘要：{len(recent_data)}条记录"
        
        prompt = f"""
# A股技术面量化分析指令（机构级标准）
你是拥有10年以上A股量化投研经验的技术分析专家，需基于{stock_code}的最新30天行情数据（含完整量化指标），完成多维度专业分析，要求逻辑可验证、结论有数据支撑、适配A股交易规则（T+1、涨跌停、主力资金等）。

## 基础数据（近30天核心量化指标）
{data_str}

## 分析维度（必须全覆盖，分点输出，每个维度附具体数据佐证）
### 1. 趋势体系分析（均线+趋势线）
- 均线排列：计算MA5/MA10/MA20/MA60的近5日斜率，明确判断短期（MA5/MA10）/中期（MA20/MA60）趋势方向（多头排列/空头排列/粘合整理）；
- 趋势强度：用MACD柱状体面积变化量化趋势强弱，结合ADX逻辑（替代）判断趋势持续性（MACD_hist连续增长=趋势增强）；
- 关键位置：识别近30天有效支撑位（前低/均线支撑）、压力位（前高/均线压力），标注突破/跌破的量能匹配度（成交量是否放大50%以上）。

### 2. 量价动力学分析（核心量化维度）
- 量价匹配度：计算近30天「上涨日成交量均值/下跌日成交量均值」，判断量价是否背离（比值>1.2=健康，<0.8=背离）；
- 换手率分析：统计近30天换手率均值，对比A股全市场均值（2%-3%）判断筹码活跃度（>5%=高活跃，<1%=低活跃）；
- 量能层级：分析近30天成交量分位数（如70%分位=量能充足），判断资金参与度。

### 3. 震荡指标信号（多指标交叉验证）
- RSI指标：分析RSI(6/12)数值区间（>70=超买，<30=超卖），标注背离信号（价格新高但RSI未新高=顶背离，反之底背离）；
- KDJ指标：分析K/D/J三线交叉形态（金叉/死叉），结合J值区间（>100=超买，<0=超卖），给出信号有效性评分（0-10分）；
- BOLL带：判断价格在布林带的位置（上轨/中轨/下轨），结合带宽收缩/扩张，预判后续波动率变化（带宽收缩=即将突破）。

### 4. 资金行为分析（A股特色维度）
- 筹码活跃度：通过换手率变化（近5日均值/近30日均值）判断主力资金是否进场；
- 量能异动：识别近30天单笔成交量超5日均量2倍以上的日期，分析异动后的价格走势；
- 尾盘行为：统计近30天尾盘30分钟涨跌幅与成交量关系，判断主力尾盘操作意图（吸筹/出货）。

### 5. 交易策略建议（可落地的量化结论）
- 多空结论：明确给出「看多/看空/震荡」核心观点，附至少3个量化数据支撑（如「看多：MA5斜率+2.3%、MACD_hist连续5日增长、RSI=45未超买」）；
- 入场条件：给出具体入场价格区间、量能要求（如「入场价180-185元，成交量需≥5日均量1.2倍」）；
- 风控规则：止损点位（最大回撤≤5%）、止盈目标（风险收益比≥1:2）、持仓周期建议；
- 风险提示：标注技术面潜在风险点（如假突破、量能不足、指标背离），并给出应对策略。

## 输出格式要求
1. 每个分析维度单独成段，小标题加粗，核心数据用【】标注（如【MA5斜率+2.3%】）；
2. 拒绝模糊表述（如「走势较好」），所有结论必须绑定具体数值；
3. 语言风格：专业、简洁、量化，适配机构投研报告格式，避免口语化；
4. 总字数控制在800-1200字，重点突出，无关信息省略；
5. 纯文本输出，无需markdown格式（方便后续整合报告）。
"""
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"{stock_code}技术面分析报告生成完成")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}技术面报告失败：{str(e)}"
            logger.error(error_msg)
            return f"⚠️ {error_msg}"

def tech_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    tech_data = state.get("tech_data", None)
    agent = TechAnalyzerAgent(model_name=DEFAULT_MODEL)
    tech_report = agent.analyze_tech(stock_code, tech_data)
    state["tech_report"] = tech_report
    return state

if __name__ == "__main__":
    try:
        agent = TechAnalyzerAgent(model_name=DEFAULT_MODEL)
        report = agent.analyze_tech("600519")
        print("\n" + "="*80)
        print("【600519 技术面分析报告】")
        print("="*80)
        print(report)
    except Exception as e:
        print(f"\n❌ 分析执行失败：{e}")
        print("\n💡 排查建议：")
        print("1. 检查.env文件中对应模型的API Key是否配置正确")
        print("2. 确认股票代码为有效A股代码（如600519）")
        print("3. 确保已安装所有依赖：pip install akshare pandas openai")
