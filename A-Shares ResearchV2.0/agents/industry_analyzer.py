import sys
import logging
from pathlib import Path
from typing import Dict, Optional

# 统一路径处理
sys.path.append(str(Path(__file__).parent.parent))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("行业面Agent")

from config.env_config import env_config
from openai import OpenAI
from agents.data_fetcher import DataFetcherAgent

# 提取常量
DEFAULT_MODEL = "qwen"
REPORT_MAX_TOKENS = 800  # 行业报告最大字数
LLM_TEMPERATURE = 0.5  # 生成温度

# 统一模型配置（补齐缺失的模型，和其他Agent一致）
MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"}
}

class IndustryAnalyzerAgent:
    """行业景气度Agent - 判断赛道优劣、行业周期（机构级）"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = self._init_llm_client()
        self.data_fetcher = data_fetcher

    def _init_llm_client(self) -> OpenAI:
        """初始化大模型客户端（统一配置，完善校验）"""
        if self.model_name not in MODEL_CONFIG_MAP:
            raise ValueError(f"❌ 暂不支持的模型：{self.model_name}")
        
        config = MODEL_CONFIG_MAP[self.model_name]
        if not config["api_key"]:
            raise ValueError(f"❌ {self.model_name}模型的API Key未配置！")
        
        return OpenAI(api_key=config["api_key"], base_url=config["base_url"])

    def _get_model_id(self) -> str:
        """获取模型ID（统一逻辑）"""
        return MODEL_CONFIG_MAP[self.model_name]["model_id"]

    def analyze_industry(self, stock_code: str, basic_info: Optional[Dict] = None) -> str:
        """生成机构级行业景气度分析报告"""
        # 1. 获取行业数据（优先使用传入的数据，否则自己获取）
        if basic_info is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            basic_info = self.data_fetcher.get_basic_info()
        
        industry = basic_info.get("行业", "未知")
        data = {"industry": industry}
        
        # 2. 处理异常
        if data.get("error"):
            return f"⚠️ 行业分析失败：{data['error']}"
        
        # 3. 构造Prompt（优化格式，常量替代魔法数字）
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
        
        # 4. 调用LLM生成报告
        try:
            completion = self.client.chat.completions.create(
                model=self._get_model_id(),
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

# 供LangGraph调用的节点函数
def industry_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    basic_info = state.get("basic_info", None)
    agent = IndustryAnalyzerAgent(DEFAULT_MODEL)
    industry_report = agent.analyze_industry(stock_code, basic_info)
    state["industry_report"] = industry_report
    return state

# 测试代码（完善异常捕获）
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