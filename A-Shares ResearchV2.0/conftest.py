"""pytest 全局配置：确保项目根目录在 sys.path，便于 `import layers` / `import config` / `import app`。

说明：
- akshare 在项目中均为函数内懒导入，顶层只依赖 pandas/requests（已在托管运行时安装），
  因此导入 layers 不需要联网或安装 akshare。
- 网络/LLM 相关测试统一用 @pytest.mark.integration 标记，默认通过 addopts=-m "not integration" 跳过。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
