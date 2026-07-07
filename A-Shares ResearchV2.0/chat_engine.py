"""
对话引擎 - 多模式路由
- report 模式：基于分析报告的上下文追问（原功能）
- live 模式：通用股票实时问答，无需先跑分析（方向1）
- plain 模式：纯 LLM 问答，投资知识/概念类问题（方向4简单版）
"""
import re
import logging
from typing import Dict, List, Optional

import pandas as pd

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from db import cache_get_latest

logger = logging.getLogger("ChatEngine")

CHAT_MAX_HISTORY = 10  # 最多保留的对话轮次
CHAT_MAX_TOKENS = 1500
CHAT_TEMPERATURE = 0.5

# 股票代码提取正则：支持 sh600519 / sz002272 / 600519 / 002272 / 600519.SH
# 注意：不用 \b，因为 \b 在数字与中文之间不工作；改用 (?<!\d)...(?!\d) 确保不匹配更长数字串
STOCK_CODE_PATTERNS = [
    re.compile(r"(?<!\d)(sh|sz)(\d{6})(?!\d)", re.IGNORECASE),          # sh600519
    re.compile(r"(?<!\d)(\d{6})\.(SH|SZ)(?!\d)", re.IGNORECASE),        # 600519.SH
    re.compile(r"(?<!\d)(60[0135]\d{3}|688\d{3}|689\d{3})(?!\d)"),      # 沪市纯数字（6位）
    re.compile(r"(?<!\d)(00[0123]\d{3}|30\d{4})(?!\d)"),                # 深市/创业板纯数字（6位）
]


def extract_stock_code(text: str) -> Optional[str]:
    """从用户问题中提取股票代码，返回标准格式（sh600519 / sz002272）"""
    for pattern in STOCK_CODE_PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            if len(groups) >= 2 and groups[0].lower() in ("sh", "sz"):
                return groups[0].lower() + groups[1]
            if len(groups) >= 2 and groups[1].upper() in ("SH", "SZ"):
                return groups[1].lower() + groups[0]
            # 纯数字，根据规则加前缀
            code = groups[0]
            if code.startswith(("600", "601", "603", "605", "688", "689")):
                return "sh" + code
            return "sz" + code
    return None


class ChatEngine:

    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)

    def chat(self, stock_code: str, messages: List[Dict], new_message: str) -> str:
        """
        对话入口 - 自动路由到对应模式
        - stock_code 可为空（前端未传，或用户未分析直接提问）
        - 自动从 new_message 中提取股票代码
        """
        # 1. 确定要分析的股票代码：优先用前端传入的，其次从问题中提取
        target_code = stock_code or extract_stock_code(new_message) or ""

        # 2. 路由决策
        if target_code:
            # 有股票代码 → 检查是否有分析报告缓存
            cache = cache_get_latest(target_code) if target_code else None
            if cache and (cache.get("context_text") or cache.get("report_markdown")):
                # 有分析报告 → report 模式
                return self._chat_report(target_code, cache, messages, new_message)
            # 无分析报告 → live 模式（实时查询数据）
            return self._chat_live(target_code, messages, new_message)

        # 3. 无股票代码 → plain 模式（纯 LLM 问答）
        return self._chat_plain(messages, new_message)

    # ==================== report 模式（原功能） ====================

    def _chat_report(self, stock_code: str, cache: Dict, messages: List[Dict], new_message: str) -> str:
        """基于分析报告的上下文追问"""
        context_text = cache.get("context_text", "")
        if not context_text:
            context_text = cache.get("report_markdown", "")[:6000]

        system_prompt = self._build_report_prompt(stock_code, context_text)
        return self._call_llm(system_prompt, messages, new_message, stock_code, "report")

    def _build_report_prompt(self, stock_code: str, context: str) -> str:
        return f"""你是一位专业的A股投资研究助手。用户正在针对股票 {stock_code} 的分析报告进行追问。

以下是该股票的最新投研分析报告内容，请基于这些数据回答用户的问题：

---
{context}
---

回答规则：
1. 基于上述分析数据回答问题，不要编造数据之外的信息
2. 回答简洁专业，使用Markdown格式
3. 如果问题涉及具体操作建议，必须附上风险提示
4. 如果用户问到了分析报告中没有覆盖的内容，诚实告知数据不足
5. 用中文回答"""

    # ==================== live 模式（方向1：通用股票实时问答） ====================

    def _chat_live(self, stock_code: str, messages: List[Dict], new_message: str) -> str:
        """通用股票实时问答 - 无需先跑分析，直接查询实时数据"""
        try:
            from layers.connectors.data_connector import DataConnector
            connector = DataConnector(stock_code)
            live_data = self._collect_live_data(connector)
            system_prompt = self._build_live_prompt(stock_code, live_data)
            return self._call_llm(system_prompt, messages, new_message, stock_code, "live")
        except Exception as e:
            logger.error(f"[ChatEngine] live模式数据获取失败: {stock_code} | {str(e)[:100]}")
            # 数据获取失败 → 降级到 plain 模式
            return self._chat_plain(messages, new_message)

    def _collect_live_data(self, connector) -> str:
        """收集实时数据，组装成 LLM 可读的文本"""
        lines = []
        try:
            val = connector.fetch_valuation_data()
            if val and not val.get("error"):
                price = val.get("price")
                pe = val.get("pe_ttm")
                pb = val.get("pb")
                pe_avg = val.get("pe_10_avg")
                lines.append("【估值数据】")
                lines.append(f"- 最新股价: {price if price else '暂不可用'}")
                if pe is not None:
                    if pe < 0:
                        lines.append(f"- PE_TTM: {pe:.2f}（公司亏损，PE不适用）")
                    else:
                        lines.append(f"- PE_TTM: {pe:.2f}")
                else:
                    lines.append("- PE_TTM: 暂不可用")
                lines.append(f"- PB: {pb if pb else '暂不可用'}")
                if pe_avg:
                    lines.append(f"- PE近2年均值: {pe_avg}")
        except Exception as e:
            lines.append(f"【估值数据】获取失败: {str(e)[:50]}")

        try:
            fina = connector.fetch_financial_data()
            if fina and not fina.get("error"):
                lines.append("\n【财务数据】")
                lines.append(f"- ROE: {fina.get('roe') if fina.get('roe') is not None else '暂不可用'}")
                gp = fina.get("gross_profit")
                lines.append(f"- 毛利率: {gp if gp is not None else '暂不可用'}")
                np_yoy = fina.get("net_profit")
                lines.append(f"- 净利润同比: {np_yoy if np_yoy is not None else '暂不可用'}")
                lines.append(f"- 流动比率: {fina.get('current_ratio') if fina.get('current_ratio') else 'API未提供'}")
                lines.append(f"- 速动比率: {fina.get('quick_ratio') if fina.get('quick_ratio') else 'API未提供'}")
        except Exception as e:
            lines.append(f"\n【财务数据】获取失败: {str(e)[:50]}")

        try:
            tech_df = connector.fetch_tech_data()
            if tech_df is not None and not tech_df.empty:
                latest = tech_df.iloc[-1]
                lines.append("\n【技术指标（最新）】")
                for col in ["close", "ma5", "ma10", "ma20", "ma60", "macd", "rsi", "k", "d", "j"]:
                    if col in latest.index and pd.notna(latest[col]):
                        val = latest[col]
                        lines.append(f"- {col.upper()}: {round(float(val), 4) if isinstance(val, (int, float)) else val}")
                if "turnover" in latest.index and pd.notna(latest["turnover"]):
                    lines.append(f"- 换手率: {round(float(latest['turnover']), 4)}%")
        except Exception as e:
            lines.append(f"\n【技术指标】获取失败: {str(e)[:50]}")

        return "\n".join(lines) if lines else "实时数据暂不可用"

    def _build_live_prompt(self, stock_code: str, live_data: str) -> str:
        return f"""你是一位专业的A股投资研究助手。用户询问股票 {stock_code} 的实时情况。

以下是该股票的实时数据（由系统自动查询）：

---
{live_data}
---

回答规则：
1. 基于上述实时数据回答问题，数据为"暂不可用"或"API未提供"时如实告知
2. 回答简洁专业，使用Markdown格式
3. 如需深度分析（多维度评分、行业对标等），建议用户点击"开始分析"生成完整报告
4. 如果问题涉及具体操作建议，必须附上风险提示
5. 用中文回答"""

    # ==================== plain 模式（方向4：纯 LLM 问答） ====================

    def _chat_plain(self, messages: List[Dict], new_message: str) -> str:
        """纯 LLM 问答 - 投资知识/概念类问题，无需数据查询"""
        system_prompt = self._build_plain_prompt()
        return self._call_llm(system_prompt, messages, new_message, "", "plain")

    def _build_plain_prompt(self) -> str:
        return """你是一位专业的A股投资研究助手，擅长解答投资知识、概念解释、市场常识类问题。

回答规则：
1. 回答简洁专业，使用Markdown格式
2. 如果用户询问具体某只股票的数据（如"茅台的PE是多少"），请提示用户在输入框中包含股票代码（如 600519 或 sh600519），系统会自动查询实时数据
3. 如果问题涉及具体操作建议，必须附上风险提示
4. 用中文回答"""

    # ==================== 公共 LLM 调用 ====================

    def _call_llm(self, system_prompt: str, messages: List[Dict], new_message: str,
                  stock_code: str, mode: str) -> str:
        """统一的 LLM 调用逻辑"""
        api_messages = [{"role": "system", "content": system_prompt}]

        # 最近N轮历史
        recent = [m for m in messages if m.get("role") in ("user", "assistant")]
        recent = recent[-(CHAT_MAX_HISTORY * 2):]
        for m in recent:
            api_messages.append({"role": m["role"], "content": m["content"]})

        api_messages.append({"role": "user", "content": new_message})

        code_label = stock_code if stock_code else "无"
        logger.info(f"[ChatEngine] {mode}模式 | 股票={code_label} | 历史轮次={len(recent)//2} | 问题={new_message[:50]}...")

        try:
            response = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=api_messages,
                temperature=CHAT_TEMPERATURE,
                max_tokens=CHAT_MAX_TOKENS,
            )
            reply = response.choices[0].message.content
            logger.info(f"[ChatEngine] {mode}模式回复成功 | 长度={len(reply)}")
            return reply
        except Exception as e:
            logger.error(f"[ChatEngine] LLM调用失败: {e}")
            return f"抱歉，对话服务暂时不可用：{str(e)[:100]}"


chat_engine = ChatEngine()
