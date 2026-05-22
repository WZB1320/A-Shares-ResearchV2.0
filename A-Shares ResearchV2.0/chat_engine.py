"""
对话引擎 - 仅支持上下文对话（分析某只股票后追问）
"""
import logging
from typing import Dict, List, Optional

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from db import cache_get_latest

logger = logging.getLogger("ChatEngine")

CHAT_MAX_HISTORY = 10  # 最多保留的对话轮次
CHAT_MAX_TOKENS = 1500
CHAT_TEMPERATURE = 0.5


class ChatEngine:

    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)

    def chat(self, stock_code: str, messages: List[Dict], new_message: str) -> str:
        """
        基于分析上下文的追问
        messages: 前端传来的完整对话历史 [{role, content}, ...]
        new_message: 用户最新提问
        返回: AI回复文本（Markdown格式）
        """
        if not stock_code or not new_message:
            return "请先完成股票分析后再提问。"

        # 加载最新分析上下文
        cache = cache_get_latest(stock_code)
        if not cache:
            return f"未找到 {stock_code} 的最新分析结果，请先生成分析报告。"

        context_text = cache.get("context_text", "")
        if not context_text:
            # 兜底：用报告markdown作为上下文
            context_text = cache.get("report_markdown", "")[:6000]

        system_prompt = self._build_system_prompt(stock_code, context_text)

        # 构建消息列表
        api_messages = [{"role": "system", "content": system_prompt}]

        # 最近N轮历史（过滤system消息，只保留user/assistant）
        recent = [m for m in messages if m.get("role") in ("user", "assistant")]
        recent = recent[-(CHAT_MAX_HISTORY * 2):]
        for m in recent:
            api_messages.append({"role": m["role"], "content": m["content"]})

        # 当前用户消息
        api_messages.append({"role": "user", "content": new_message})

        logger.info(f"[ChatEngine] 对话请求: {stock_code} | 历史轮次: {len(recent)//2} | 问题: {new_message[:50]}...")

        try:
            response = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=api_messages,
                temperature=CHAT_TEMPERATURE,
                max_tokens=CHAT_MAX_TOKENS,
            )
            reply = response.choices[0].message.content
            logger.info(f"[ChatEngine] 对话回复成功: {stock_code} | 长度: {len(reply)}")
            return reply
        except Exception as e:
            logger.error(f"[ChatEngine] LLM调用失败: {e}")
            return f"抱歉，对话服务暂时不可用：{str(e)[:100]}"

    def _build_system_prompt(self, stock_code: str, context: str) -> str:
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


chat_engine = ChatEngine()