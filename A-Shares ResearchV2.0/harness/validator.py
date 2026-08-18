"""
harness/validator.py —— 兼容垫片（2026-08-18 合并后）。

原 HarnessValidator / ValidationResult / harness_validator 已统一迁移到
layers/validators（单一实现源），此处仅做 re-export，保证以下旧引用继续可用：
  - harness/__init__.py:  from harness.validator import HarnessValidator
  - harness.scheduler（历史）: from harness.validator import harness_validator, ValidationResult
  - tests: from harness.validator import harness_validator

请勿在此文件新增逻辑；校验实现一律改在 layers/validators。
"""
from layers.validators import (
    HarnessValidator,
    ValidationResult,
    harness_validator,
)

__all__ = ["HarnessValidator", "ValidationResult", "harness_validator"]
