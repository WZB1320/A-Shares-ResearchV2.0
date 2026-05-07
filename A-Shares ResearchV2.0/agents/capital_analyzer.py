import sys
import logging
from pathlib import Path
from typing import Dict, Optional

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("资金面Agent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from agents.data_fetcher import DataFetcherAgent

CAPITAL_DATA_DAYS = 30
REPORT_MAX_TOKENS = 800
LLM_TEMPERATURE = 0.5

class CapitalAnalyzerAgent:
    """资金面分析Agent - 追踪北向/主力/游资资金动向（机构级）"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_fetcher = data_fetcher

    def analyze_capital(self, stock_code: str, capital_data: Optional[Dict] = None) -> str:
        """生成机构级资金面分析报告（处理异常数据）"""
        data = capital_data
        if data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            data = self.data_fetcher.get_capital_data()
        
        if data.get("error"):
            return f"⚠️ 资金面分析失败：{data['error']}"
        
        prompt = f"""
# A股资金面量化分析（机构级）
分析{stock_code}近{CAPITAL_DATA_DAYS}天资金流向，所有结论带数值【】
1. 北向资金：持仓变化、净流入、增减持强度
2. 主力资金：大单净流入、占比、尾盘异动
3. 龙虎榜：机构/游资买卖行为
4. 融资融券：杠杆资金情绪
5. 结论：资金主导方向（主力吸筹/出货/观望）
纯文本，{REPORT_MAX_TOKENS}字内，数据用【】标注

## 数据参考
- 北向资金数据: {data['north'][-5:] if len(data['north']) > 0 else '无数据'}
- 主力资金数据: {data['main'][-5:] if len(data['main']) > 0 else '无数据'}
- 融资融券数据: {data['margin'][-5:] if len(data['margin']) > 0 else '无数据'}
- 龙虎榜数据: {data['dragon'][-5:] if len(data['dragon']) > 0 else '无数据'}
        """
        
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"{stock_code}资金面分析报告生成完成")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}资金面报告失败：{str(e)}"
            logger.error(error_msg)
            return f"⚠️ {error_msg}"

def capital_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    capital_data = state.get("capital_data", None)
    agent = CapitalAnalyzerAgent(DEFAULT_MODEL)
    capital_report = agent.analyze_capital(stock_code, capital_data)
    state["capital_report"] = capital_report
    return state

if __name__ == "__main__":
    try:
        agent = CapitalAnalyzerAgent(DEFAULT_MODEL)
        report = agent.analyze_capital("600519")
        print("\n" + "="*80)
        print("【600519 机构级资金面分析报告】")
        print("="*80)
        print(report)
    except Exception as e:
        logger.error(f"执行失败：{str(e)}")
        print(f"❌ 执行失败：{str(e)}")
