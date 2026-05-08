from config.env_config import env_config
from openai import OpenAI

MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"}
}

DEFAULT_MODEL = "deepseek"

def get_llm_client(model_name: str = DEFAULT_MODEL):
    model_name = model_name.lower()
    if model_name not in MODEL_CONFIG_MAP:
        raise ValueError(f"不支持的模型：{model_name}")
    config = MODEL_CONFIG_MAP[model_name]
    if not config["api_key"]:
        raise ValueError(f"{model_name}模型的API Key未配置！")
    return OpenAI(api_key=config["api_key"], base_url=config["base_url"])

def get_model_id(model_name: str = DEFAULT_MODEL) -> str:
    model_name = model_name.lower()
    if model_name not in MODEL_CONFIG_MAP:
        raise ValueError(f"不支持的模型：{model_name}")
    return MODEL_CONFIG_MAP[model_name]["model_id"]

llm_client = get_llm_client()
