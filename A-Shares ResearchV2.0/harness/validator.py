import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger("Harness-Validator")


class ValidationResult:
    def __init__(self):
        self.passed = True
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.suggestions: List[str] = []

    def add_error(self, message: str) -> None:
        self.passed = False
        self.errors.append(message)
        logger.error(f"[Validator] 错误: {message}")

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)
        logger.warning(f"[Validator] 警告: {message}")

    def add_suggestion(self, message: str) -> None:
        self.suggestions.append(message)
        logger.info(f"[Validator] 建议: {message}")

    def is_valid(self) -> bool:
        return self.passed

    def get_summary(self) -> str:
        status = "✅ 通过" if self.passed else "❌ 失败"
        summary = f"[{status}]"
        if self.errors:
            summary += f" 错误数: {len(self.errors)}"
        if self.warnings:
            summary += f" 警告数: {len(self.warnings)}"
        if self.suggestions:
            summary += f" 建议数: {len(self.suggestions)}"
        return summary

    def __str__(self) -> str:
        lines = [self.get_summary()]
        if self.errors:
            lines.append("\n【错误】")
            for e in self.errors:
                lines.append(f"  - {e}")
        if self.warnings:
            lines.append("\n【警告】")
            for w in self.warnings:
                lines.append(f"  - {w}")
        if self.suggestions:
            lines.append("\n【建议】")
            for s in self.suggestions:
                lines.append(f"  - {s}")
        return "\n".join(lines)


class HarnessValidator:
    def __init__(self):
        self.validation_rules: Dict[str, callable] = {}

    def validate_data_completeness(self, data: Dict[str, Any], required_fields: List[str]) -> ValidationResult:
        result = ValidationResult()

        for field in required_fields:
            if field not in data or data[field] is None:
                result.add_error(f"缺少必填数据字段: {field}")
            elif data[field] == "" or data[field] == {} or data[field] == []:
                result.add_error(f"数据字段为空: {field}")

        if result.is_valid():
            logger.info(f"[Validator] 数据完整性检查通过 | 字段数: {len(required_fields)}")

        return result

    def validate_stock_code(self, stock_code: str) -> ValidationResult:
        result = ValidationResult()

        if not stock_code:
            result.add_error("股票代码不能为空")
            return result

        stock_code = stock_code.strip()

        if not stock_code.isdigit():
            result.add_error(f"股票代码必须为数字，当前值: {stock_code}")
        elif len(stock_code) not in [6, 7]:
            result.add_warning(f"股票代码长度异常，当前值: {stock_code} (通常为6位)")
        else:
            prefix = stock_code[:3]
            if prefix.startswith(("600", "601", "603", "605")):
                logger.info(f"[Validator] 上交所主板股票: {stock_code}")
            elif prefix.startswith(("000", "001", "002", "003")):
                logger.info(f"[Validator] 深交所股票: {stock_code}")
            elif prefix.startswith(("688")):
                logger.info(f"[Validator] 科创板股票: {stock_code}")
            elif prefix.startswith(("8", "4")):
                logger.info(f"[Validator] 北交所股票: {stock_code}")

        return result

    def validate_valuation(self, valuation_data: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()

        pe_ttm = valuation_data.get("pe_ttm", 0)
        pb = valuation_data.get("pb", 0)

        if pe_ttm and isinstance(pe_ttm, (int, float)):
            if pe_ttm < 0:
                result.add_warning(f"PE为负数: {pe_ttm} (可能表示公司亏损)")
            elif pe_ttm > 100:
                result.add_warning(f"PE过高: {pe_ttm} (可能存在估值泡沫)")
            elif pe_ttm < 10:
                result.add_suggestion(f"PE较低: {pe_ttm} (可能被低估)")
            else:
                logger.info(f"[Validator] PE处于合理区间: {pe_ttm}")

        if pb and isinstance(pb, (int, float)):
            if pb < 0:
                result.add_error(f"PB为负数: {pb} (异常值)")
            elif pb > 10:
                result.add_warning(f"PB过高: {pb} (可能存在估值泡沫)")
            elif pb < 1:
                result.add_suggestion(f"PB低于1: {pb} (可能被低估或资产质量问题)")
            else:
                logger.info(f"[Validator] PB处于合理区间: {pb}")

        return result

    def validate_price(self, price: float, price_history: List[float]) -> ValidationResult:
        result = ValidationResult()

        if not price or price <= 0:
            result.add_error(f"当前价格异常: {price}")
            return result

        if price_history and len(price_history) > 0:
            avg_price = sum(price_history) / len(price_history)
            deviation = abs(price - avg_price) / avg_price * 100

            if deviation > 50:
                result.add_warning(f"当前价格偏离历史均值过大: {deviation:.1f}% (当前: {price}, 均值: {avg_price:.2f})")
            elif deviation > 20:
                result.add_suggestion(f"当前价格偏离历史均值: {deviation:.1f}%")
            else:
                logger.info(f"[Validator] 价格处于合理区间: {price}")

        return result

    def validate_risk_metrics(self, risk_data: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()

        volatility = risk_data.get("volatility")
        if volatility and isinstance(volatility, (int, float)):
            if volatility > 0.5:
                result.add_warning(f"波动率过高: {volatility:.2%} (高风险)")
            elif volatility > 0.3:
                result.add_suggestion(f"波动率偏高: {volatility:.2%}")
            else:
                logger.info(f"[Validator] 波动率正常: {volatility:.2%}")

        beta = risk_data.get("beta")
        if beta and isinstance(beta, (int, float)):
            if beta > 2:
                result.add_warning(f"Beta值过高: {beta} (与市场相关性过强)")
            elif beta < 0:
                result.add_warning(f"Beta值为负: {beta} (与市场负相关)")
            else:
                logger.info(f"[Validator] Beta值正常: {beta}")

        return result

    def validate_report_content(self, report: str, min_length: int = 100) -> ValidationResult:
        result = ValidationResult()

        if not report or not isinstance(report, str):
            result.add_error("报告内容为空或格式错误")
            return result

        if len(report) < min_length:
            result.add_error(f"报告内容过短: {len(report)} 字符 (最少 {min_length})")

        if "错误" in report or "失败" in report or "异常" in report:
            result.add_warning("报告包含异常/错误关键词，请人工复核")

        keywords = ["分析", "数据", "结论", "建议"]
        missing_keywords = [kw for kw in keywords if kw not in report]
        if len(missing_keywords) > 2:
            result.add_suggestion(f"报告可能缺少分析框架关键词: {missing_keywords}")

        return result

    def validate_financial_data(self, financial_data: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()

        roe = financial_data.get("roe")
        if roe and isinstance(roe, (int, float)):
            if roe < 0:
                result.add_warning(f"ROE为负: {roe}% (公司亏损)")
            elif roe < 5:
                result.add_suggestion(f"ROE偏低: {roe}%")
            elif roe > 20:
                logger.info(f"[Validator] ROE优秀: {roe}%")
            else:
                logger.info(f"[Validator] ROE正常: {roe}%")

        gross_profit = financial_data.get("gross_profit")
        if gross_profit and isinstance(gross_profit, (int, float)):
            if gross_profit < 0:
                result.add_error(f"毛利率为负: {gross_profit}%")
            elif gross_profit < 20:
                result.add_warning(f"毛利率偏低: {gross_profit}%")
            elif gross_profit > 50:
                logger.info(f"[Validator] 毛利率优秀: {gross_profit}%")

        return result

    def cross_validate(self, data: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()

        if "price" in data and "pe_ttm" in data and "eps" in data:
            price = data["price"]
            pe_ttm = data["pe_ttm"]
            eps = data.get("eps")

            if eps and isinstance(eps, (int, float)) and eps > 0:
                calculated_pe = price / eps
                if abs(calculated_pe - pe_ttm) > 5:
                    result.add_warning(f"PE计算值({calculated_pe:.2f})与报告值({pe_ttm})差异较大")

        if "price" in data and "pb" in data and "book_value" in data:
            price = data["price"]
            pb = data["pb"]
            book_value = data.get("book_value")

            if book_value and isinstance(book_value, (int, float)) and book_value > 0:
                calculated_pb = price / book_value
                if abs(calculated_pb - pb) > 2:
                    result.add_warning(f"PB计算值({calculated_pb:.2f})与报告值({pb})差异较大")

        return result

    def validate_all(self, state_data: Dict[str, Any]) -> Dict[str, ValidationResult]:
        results = {}

        if "stock_code" in state_data:
            results["stock_code"] = self.validate_stock_code(state_data["stock_code"])

        if "valuation" in state_data:
            results["valuation"] = self.validate_valuation(state_data["valuation"])

        if "risk" in state_data:
            results["risk"] = self.validate_risk_metrics(state_data["risk"])

        if "financial" in state_data:
            results["financial"] = self.validate_financial_data(state_data["financial"])

        return results


harness_validator = HarnessValidator()
