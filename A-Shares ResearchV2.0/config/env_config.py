# config/env_config.py
from dotenv import load_dotenv
import os

# 加载本地.env文件（优先加载）
load_dotenv()

class EnvConfig:
    """AI投研系统 - 多模型统一配置类（适配你的原有配置）"""
    
    # ===== 1. DeepSeek 深度求索 =====
    DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
    DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
    
    # ===== 2. 阿里云 通义千问 (Qwen) =====
    QWEN_API_KEY = os.getenv("QWEN_API_KEY", "")
    QWEN_BASE_URL = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    
    # ===== 3. 百度 文心一言 (ERNIE) =====
    ERNIE_API_KEY = os.getenv("ERNIE_API_KEY", "")
    ERNIE_BASE_URL = os.getenv("ERNIE_BASE_URL", "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions")
    
    # ===== 4. 字节跳动 豆包 (Doubao) =====
    DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
    DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
    
    # ===== 5. 讯飞星火 (Spark) =====
    SPARK_API_KEY = os.getenv("SPARK_API_KEY", "")
    SPARK_BASE_URL = os.getenv("SPARK_BASE_URL", "https://spark-api.xf-yun.com/v1.1/chat")
    
    # ===== 6. 腾讯 混元 (Hunyuan) =====
    HUNYUAN_API_KEY = os.getenv("HUNYUAN_API_KEY", "")
    HUNYUAN_BASE_URL = os.getenv("HUNYUAN_BASE_URL", "https://hunyuan.tencentcloudapi.com")
    
    # ===== 数据接口配置 =====
    # akshare超时时间（确保是数字，默认30秒）
    try:
        AKSHARE_TIMEOUT = int(os.getenv("AKSHARE_TIMEOUT", 30))
    except ValueError:
        raise ValueError("AKSHARE_TIMEOUT必须是数字！请检查.env文件")
    TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

    # 可选：快速检查模型配置是否完整（比如用通义千问时，校验Key是否为空）
    def check_model_config(self, model_name: str) -> bool:
        """检查指定模型的API Key是否配置"""
        if model_name.lower() == "qwen":
            return bool(self.QWEN_API_KEY)
        elif model_name.lower() == "deepseek":
            return bool(self.DEEPSEEK_API_KEY)
        elif model_name.lower() == "ernie":
            return bool(self.ERNIE_API_KEY)
        elif model_name.lower() == "doubao":
            return bool(self.DOUBAO_API_KEY)
        elif model_name.lower() == "spark":
            return bool(self.SPARK_API_KEY)
        elif model_name.lower() == "hunyuan":
            return bool(self.HUNYUAN_API_KEY)
        else:
            return False

# 创建配置实例，其他文件直接导入使用
env_config = EnvConfig()

# 校验模型配置的辅助函数
def check_model_config(model_name: str) -> bool:
    if model_name == "qwen":
        return bool(env_config.QWEN_API_KEY)
    elif model_name == "deepseek":
        return bool(env_config.DEEPSEEK_API_KEY)
    elif model_name == "ernie":
        return bool(env_config.ERNIE_API_KEY)
    else:
        return False