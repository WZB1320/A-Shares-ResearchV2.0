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
logger = logging.getLogger("资金面Agent")

from config.env_config import env_config
from openai import OpenAI
from agents.data_fetcher import DataFetcherAgent

# 提取常量
DEFAULT_MODEL = "qwen"
CAPITAL_DATA_DAYS = 30  # 资金数据采集天数
REPORT_MAX_TOKENS = 800  # 资金报告最大字数
LLM_TEMPERATURE = 0.5  # 生成温度

# 统一模型配置（和基本面Agent保持一致）
MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"}
}

class CapitalAnalyzerAgent:
    """资金面分析Agent - 追踪北向/主力/游资资金动向（机构级）"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = self._init_llm_client()
        self.data_fetcher = data_fetcher

    def _init_llm_client(self) -> OpenAI:
        """初始化大模型客户端（统一配置）"""
        if self.model_name not in MODEL_CONFIG_MAP:
            raise ValueError(f"❌ 暂不支持的模型：{self.model_name}")
        
        config = MODEL_CONFIG_MAP[self.model_name]
        if not config["api_key"]:
            raise ValueError(f"❌ {self.model_name}模型的API Key未配置！")
        
        return OpenAI(api_key=config["api_key"], base_url=config["base_url"])

    def _get_model_id(self) -> str:
        """获取模型ID（统一从配置映射读取）"""
        return MODEL_CONFIG_MAP[self.model_name]["model_id"]

    def analyze_capital(self, stock_code: str, capital_data: Optional[Dict] = None) -> str:
        """生成机构级资金面分析报告（处理异常数据）"""
        # 1. 获取资金数据（优先使用传入的数据，否则自己获取）
        data = capital_data
        if data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            data = self.data_fetcher.get_capital_data()
        
        # 2. 处理异常数据（避免错误信息传入Prompt）
        if data.get("error"):
            return f"⚠️ 资金面分析失败：{data['error']}"
        
        # 3. 构造Prompt（优化格式，常量替代魔法数字）
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
        
        # 4. 调用LLM生成报告
        try:
            completion = self.client.chat.completions.create(
                model=self._get_model_id(),
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

# 供LangGraph调用的节点函数
def capital_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    capital_data = state.get("capital_data", None)
    agent = CapitalAnalyzerAgent(DEFAULT_MODEL)
    capital_report = agent.analyze_capital(stock_code)
    state["capital_report"] = capital_report
    return state

# 测试代码（完善异常捕获）
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