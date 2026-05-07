import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("基本面Agent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from config.env_config import env_config
from agents.data_fetcher import DataFetcherAgent
import tushare as ts

RECENT_YEARS = 3
INDUSTRY_STOCK_LIMIT = 10
LLM_TIMEOUT = 60
LLM_TEMPERATURE = 0.5
REPORT_MAX_TOKENS = 1500

class FundAnalyzerAgent:
    """基本面分析Agent类（支持多模型/财务数据/估值分析）"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.ts_pro = self._init_tushare()
        self.data_fetcher = data_fetcher

    def _init_tushare(self) -> Optional[ts.pro_api]:
        try:
            if env_config.TUSHARE_TOKEN:
                ts.set_token(env_config.TUSHARE_TOKEN)
                return ts.pro_api()
        except Exception as e:
            logger.warning(f"Tushare初始化失败: {e}")
        return None

    def analyze_fund(self, stock_code: str, fundamental_data: Optional[Dict] = None) -> str:
        """生成机构级基本面分析报告"""
        fund_data = fundamental_data
        if fund_data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            fund_data = self.data_fetcher.get_fundamental_data()
        
        prompt = f"""
# A股基本面量化分析指令（公募基金投研标准 V2.0）
你是任职于头部公募基金的行业首席分析师，拥有15年A股消费/制造/科技等行业基本面研究经验，需基于{stock_code}的完整财务&行业数据，完成「量化可验证、逻辑可推导、结论可落地」的机构级基本面分析。

## 基础数据层
### 1. 公司基本画像
{fund_data['basic_info']}

### 2. 核心财务数据（近{RECENT_YEARS}年合并报表）
{fund_data['finance']}

### 3. 估值矩阵（绝对+相对）
{fund_data['valuation']}

### 4. 行业对标池（申万二级行业前{INDUSTRY_STOCK_LIMIT}）
{fund_data['industry_stocks']}

## 分析维度（必须全覆盖，每个维度附量化指标+行业分位）
### 1. 盈利能力深度拆解（量化评分 0-100分）
- 盈利质量：计算近{RECENT_YEARS}年「扣非净利润/净利润」均值【≥90%=优质，<70%=存疑】、「经营现金流净额/净利润」均值【≥1=盈利真实，<0.8=现金流缺口】；
- 盈利效率：ROE杜邦拆解（净利率×资产周转率×权益乘数），对比行业中位数，标注各因子贡献度（如【净利率贡献ROE 6.2pct】）；
- 盈利持续性：毛利率/净利率近{RECENT_YEARS}年CAGR（复合增长率），判断是否跑赢行业通胀（如【毛利率CAGR 4.5% > 行业2.1%】）；
- 盈利评分：基于上述指标给出0-100分盈利能力评分，附评分依据。

### 2. 资产负债表健康度（风险量化）
- 偿债能力：计算流动比率（≥2=健康）、速动比率（≥1=健康）、资产负债率（分行业阈值：消费<40%/制造<60%/金融<90%）；
- 营运能力：应收账款周转率/存货周转率近{RECENT_YEARS}年变化，对比行业均值，判断产业链地位（如【应收周转率12次 > 行业8次=话语权强】）；
- 有息负债：有息负债/总资产占比，利息保障倍数（EBIT/利息费用≥3=安全），识别财务暴雷风险；
- 资产质量：固定资产/总资产占比，商誉/净资产占比（>20%=高商誉风险），存货跌价准备计提比例。

### 3. 成长能力量化建模
- 核心成长指标：营收/归母净利润/扣非净利润近{RECENT_YEARS}年CAGR，拆分「量增/价涨/品类扩张」贡献（如【营收增长15%=量增10%+价涨5%】）；
- 成长可持续性：研发费用率/销售费用率近{RECENT_YEARS}年变化，资本开支（CAPEX）/折旧摊销比，判断成长是内生还是外延；
- 行业成长性：公司成长速度 vs 申万二级行业营收CAGR，标注「成长性溢价/折价」（如【公司CAGR 18% > 行业12%=溢价】）；
- 成长天花板：基于行业空间（市场规模）、市占率（当前/目标）测算未来{RECENT_YEARS}年营收上限。

### 4. 估值体系建模（绝对+相对）
- 绝对估值：DCF估值（简化版）：基于近{RECENT_YEARS}年自由现金流均值、永续增长率（2-3%）、折现率（8-10%）测算内在价值；
- 相对估值：
  ① 静态估值：当前PE(TTM)/PB(LF)/PS(TTM) 对比近{RECENT_YEARS}年历史分位数（0-100%，如【PE分位65%=中性偏贵】）；
  ② 动态估值：PEG（PE/盈利CAGR），【PEG<1=低估，1-1.5=合理，>1.5=高估】；
  ③ 行业比价：公司估值 vs 行业中位数/龙头公司，计算估值溢价率（如【PE溢价15%=合理，因ROE高5pct】）；
- 估值安全边际：内在价值 vs 当前股价，测算下跌空间（如【安全边际20%=股价下跌20%后进入价值区间】）。

### 5. 行业壁垒与竞争格局
- 核心壁垒：量化分析护城河（品牌力：毛利率溢价/渠道力：终端覆盖率/技术力：专利数/成本力：单位成本低于行业均值）；
- 竞争格局：CR3/CR5（行业集中度），公司市占率近{RECENT_YEARS}年变化，判断行业处于「增量/存量/出清」阶段；
- 政策敏感度：行业政策（如集采/补贴/环保）对公司营收的影响系数（如【集采影响毛利率-5pct】）；
- 替代风险：技术迭代/消费习惯变化对核心产品的冲击概率（0-100%）。

### 6. 投资决策结论（机构级）
- 投资评级：明确「买入/增持/持有/减持/卖出」（必须附3个以上量化数据支撑）；
- 目标价测算：基于DCF估值+相对估值，给出6/12个月目标价，测算上涨空间（如【12个月目标价200元，上涨空间15%】）；
- 持仓策略：适合「长期持有/波段操作/左侧布局/右侧追涨」，附持仓周期（如【6-12个月，波段操作】）；
- 风险因子：
  ① 量化风险：业绩不及预期概率（基于盈利预测偏差）、估值回调风险（基于历史分位数）；
  ② 定性风险：管理层变动/行业政策/原材料涨价/汇率波动；
  ③ 应对策略：不同风险场景下的仓位调整建议（如【原材料涨价超10%，仓位降至50%】）。

## 输出格式强制要求
1. 每个分析维度单独成段，小标题加粗，所有核心数据用【】标注（保留2位小数）；
2. 拒绝「较好/一般/不错」等模糊表述，所有结论必须绑定量化指标（如【ROE 22.5% > 行业15.8%】）；
3. 财务分析必须体现「拆解思维」（如ROE杜邦拆解、营收增长拆分），而非简单罗列数据；
4. 语言风格：公募基金投研报告风格，专业、严谨、量化，避免口语化；
5. 总字数：1200-1500字，结构清晰，重点突出，无关信息（如公司简介）省略；
6. 纯文本输出，无需markdown格式，分点使用「-」，避免表格/图表。
        """
        
        logger.info(f"调用{self.model_name}模型生成基本面分析报告")
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS,
                timeout=LLM_TIMEOUT
            )
            report = completion.choices[0].message.content.strip()
            logger.info("基本面分析报告生成完成")
            return report
        except Exception as e:
            logger.error(f"生成{stock_code}基本面报告失败：{str(e)}")
            raise Exception(f"生成基本面报告失败：{str(e)}")

def fund_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    fundamental_data = state.get("fundamental_data", None)
    agent = FundAnalyzerAgent(model_name=DEFAULT_MODEL)
    fund_report = agent.analyze_fund(stock_code, fundamental_data)
    state["fund_report"] = fund_report
    return state

if __name__ == "__main__":
    try:
        agent = FundAnalyzerAgent(model_name=DEFAULT_MODEL)
        report = agent.analyze_fund("600519")
        print("\n" + "="*80)
        print("【600519 机构级基本面分析报告】")
        print("="*80)
        print(report)
    except Exception as e:
        logger.error(f"执行失败：{str(e)}")
        print(f"❌ 执行失败：{str(e)}")
