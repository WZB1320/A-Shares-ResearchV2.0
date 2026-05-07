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
logger = logging.getLogger("行业面Agent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from agents.data_fetcher import DataFetcherAgent

REPORT_MAX_TOKENS = 800
LLM_TEMPERATURE = 0.5

class IndustryAnalyzerAgent:
    """行业景气度Agent - 判断赛道优劣、行业周期（机构级）"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_fetcher = data_fetcher

    def analyze_industry(self, stock_code: str, basic_info: Optional[Dict] = None) -> str:
        """生成机构级行业景气度分析报告"""
        if basic_info is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            basic_info = self.data_fetcher.get_basic_info()
        
        industry = basic_info.get("行业", "未知")
        data = {"industry": industry}
        
        if data.get("error"):
            return f"⚠️ 行业分析失败：{data['error']}"
        
        prompt = f"""
# A股行业景气度分析（机构级）
{stock_code} 所属行业：{data['industry']}
分析要求：
1. 行业周期：明确标注「成长/成熟/衰退」周期阶段，附量化依据【】；
2. 景气度评分：0-100分，标注评分依据【】；
3. 行业估值分位：当前估值在近3年历史分位【】、政策利好类型及影响【】；
4. 个股β系数：测算个股相对于行业指数的弹性【】；
5. 赛道投资价值：明确「高/中/低」，附3个以上量化数据支撑【】。
输出要求：纯文本，{REPORT_MAX_TOKENS}字内，所有核心数据用【】标注，拒绝模糊表述。
        """
        
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"{stock_code}行业景气度分析报告生成完成")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}行业分析报告失败：{str(e)}"
            logger.error(error_msg)
            return f"⚠️ {error_msg}"

def industry_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    basic_info = state.get("basic_info", None)
    agent = IndustryAnalyzerAgent(DEFAULT_MODEL)
    industry_report = agent.analyze_industry(stock_code, basic_info)
    state["industry_report"] = industry_report
    return state

if __name__ == "__main__":
    try:
        agent = IndustryAnalyzerAgent(DEFAULT_MODEL)
        report = agent.analyze_industry("600519")
        print("\n" + "="*80)
        print("【600519 机构级行业景气度分析报告】")
        print("="*80)
        print(report)
    except Exception as e:
        logger.error(f"执行失败：{str(e)}")
        print(f"❌ 执行失败：{str(e)}")
