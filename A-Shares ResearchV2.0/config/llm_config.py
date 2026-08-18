import threading
import time
import logging
from config.env_config import env_config
from openai import OpenAI
from openai import (
    APITimeoutError,
    APIConnectionError,
    RateLimitError,
    InternalServerError,
)

logger = logging.getLogger("LLMClient")

MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"}
}

DEFAULT_MODEL = "deepseek"

# ===== 统一的 LLM 调用层配置 =====
LLM_TIMEOUT = 120.0          # 单次请求超时（秒）
LLM_MAX_RETRIES = 3          # 失败后最大重试次数
LLM_BACKOFF_BASE = 1.5       # 指数退避基数（秒）
LLM_BACKOFF_MAX = 15.0       # 退避上限（秒）

# 客户端按模型缓存（线程安全）
_client_cache: dict = {}
_client_lock = threading.Lock()

# token 用量统计（线程安全）
_usage_lock = threading.Lock()
_usage_stats = {
    "calls": 0,
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0,
    "errors": 0,
    "by_model": {},
}


def get_llm_client(model_name: str = DEFAULT_MODEL):
    """获取（并缓存）指定模型的 OpenAI 客户端。

    客户端关闭内置重试（max_retries=0），改由 chat_completion 统一做指数退避，
    避免客户端与服务端重试叠加导致超时过长。
    """
    model_name = model_name.lower()
    if model_name not in MODEL_CONFIG_MAP:
        raise ValueError(f"不支持的模型：{model_name}")
    config = MODEL_CONFIG_MAP[model_name]
    if not config["api_key"]:
        raise ValueError(f"{model_name}模型的API Key未配置！")
    with _client_lock:
        if model_name not in _client_cache:
            _client_cache[model_name] = OpenAI(
                api_key=config["api_key"],
                base_url=config["base_url"],
                timeout=LLM_TIMEOUT,
                max_retries=0,
            )
        return _client_cache[model_name]


def get_model_id(model_name: str = DEFAULT_MODEL) -> str:
    model_name = model_name.lower()
    if model_name not in MODEL_CONFIG_MAP:
        raise ValueError(f"不支持的模型：{model_name}")
    return MODEL_CONFIG_MAP[model_name]["model_id"]


def _record_usage(model_name: str, usage) -> None:
    """把一次调用的 token 用量累加到统计中"""
    if usage is None:
        return
    pt = getattr(usage, "prompt_tokens", 0) or 0
    ct = getattr(usage, "completion_tokens", 0) or 0
    tt = getattr(usage, "total_tokens", 0) or (pt + ct)
    with _usage_lock:
        s = _usage_stats
        s["calls"] += 1
        s["prompt_tokens"] += pt
        s["completion_tokens"] += ct
        s["total_tokens"] += tt
        bm = s["by_model"].setdefault(
            model_name,
            {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        )
        bm["calls"] += 1
        bm["prompt_tokens"] += pt
        bm["completion_tokens"] += ct
        bm["total_tokens"] += tt


# 仅针对可恢复的瞬态错误做重试（连接/超时/限流/5xx），4xx 类错误直接抛出
_RETRYABLE = (APITimeoutError, APIConnectionError, RateLimitError, InternalServerError)


def chat_completion(
    model_name: str = DEFAULT_MODEL,
    messages=None,
    temperature: float = 0.0,
    max_tokens: int = 1500,
    timeout: float = None,
    task_tag: str = "",
):
    """统一的 LLM 调用入口。

    职责：
    - 请求级超时（timeout，默认 LLM_TIMEOUT）
    - 指数退避重试（LLM_MAX_RETRIES 次，仅对瞬态错误）
    - token 用量统计（按模型汇总，见 get_usage_stats）
    - 统一返回 choices[0].message.content（已 strip）

    非瞬态异常（如鉴权失败、参数错误）会原样抛出，不做无意义重试。
    """
    model_name = (model_name or DEFAULT_MODEL).lower()
    client = get_llm_client(model_name)
    model_id = get_model_id(model_name)
    req_timeout = timeout if timeout is not None else LLM_TIMEOUT

    last_err = None
    for attempt in range(LLM_MAX_RETRIES + 1):
        try:
            completion = client.chat.completions.create(
                model=model_id,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=req_timeout,
            )
            _record_usage(model_name, completion.usage)
            return completion.choices[0].message.content.strip()
        except _RETRYABLE as e:
            last_err = e
            if attempt < LLM_MAX_RETRIES:
                sleep_t = min(LLM_BACKOFF_BASE * (2 ** attempt), LLM_BACKOFF_MAX)
                logger.warning(
                    f"[LLMClient] {task_tag or model_name} 调用失败(第{attempt + 1}次)，"
                    f"{sleep_t:.1f}s 后重试: {type(e).__name__}"
                )
                time.sleep(sleep_t)
            else:
                break
        except Exception as e:
            logger.error(f"[LLMClient] {task_tag or model_name} 调用异常: {e}")
            with _usage_lock:
                _usage_stats["errors"] += 1
            raise

    with _usage_lock:
        _usage_stats["errors"] += 1
    raise RuntimeError(
        f"LLM 调用在 {LLM_MAX_RETRIES} 次重试后仍失败: {type(last_err).__name__}: {last_err}"
    )


def get_usage_stats() -> dict:
    """返回当前 token 用量统计的副本（线程安全快照）"""
    import copy
    with _usage_lock:
        return copy.deepcopy(_usage_stats)


def reset_usage_stats() -> None:
    """清零 token 用量统计"""
    with _usage_lock:
        _usage_stats.update({
            "calls": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "errors": 0,
            "by_model": {},
        })


# 兼容旧调用的模块级单例（已被 chat_completion 取代，保留仅作向后兼容）
llm_client = get_llm_client()
