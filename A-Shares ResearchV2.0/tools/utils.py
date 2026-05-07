import logging
import functools

# 1. 统一日志配置
def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s] %(name)s: %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(ch)
    return logger

# 2. 异常捕获装饰器（所有Agent可用）
def catch_error(func):
    @functools.wraps(func)
    def wrapper(state: dict):
        try:
            return func(state)
        except Exception as e:
            logger = get_logger(func.__name__)
            logger.error(f"执行异常: {str(e)}")
            state[f"{func.__name__.replace('_node','')}_error"] = str(e)
            return state
    return wrapper

# 3. 数字格式化工具
def format_num(num, default=0.0):
    try:
        return round(float(num), 2)
    except:
        return default