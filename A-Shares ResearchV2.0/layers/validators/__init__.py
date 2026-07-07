import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger("DataValidator")


@dataclass
class FieldValidation:
    field: str
    value: Any
    status: str
    issue: str = ""
    suggestion: str = ""


@dataclass
class DimensionQuality:
    dimension: str
    score: int = 100
    grade: str = "优秀"
    fields_total: int = 0
    fields_pass: int = 0
    fields_warn: int = 0
    fields_fail: int = 0
    warnings: List[str] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)
    field_details: List[FieldValidation] = field(default_factory=list)


@dataclass
class QualityReport:
    stock_code: str
    overall_score: int = 100
    overall_grade: str = "优秀"
    dimensions: Dict[str, DimensionQuality] = field(default_factory=dict)
    global_warnings: List[str] = field(default_factory=list)
    validation_timestamp: str = ""

    def has_critical_failures(self) -> bool:
        return self.overall_score < 40

    def has_warnings(self) -> bool:
        return self.overall_score < 70

    def to_context_string(self) -> str:
        return _quality_report_to_context(self)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class DataValidator:

    TECH_REQUIRED = ["close", "volume", "ma5", "ma20", "macd", "rsi6"]
    TECH_BOUNDS = {
        "close": (0.01, 50000),
        "ma5": (0.01, 50000),
        "ma20": (0.01, 50000),
        "ma60": (0.01, 50000),
        "volume": (0, 1e12),
        "rsi6": (0, 100),
        "k": (0, 100),
        "d": (0, 100),
        "j": (-50, 150),
        "macd": (-100, 100),
        "pct_change": (-20, 20),
        "turnover": (0, 100),
        "boll_upper": (0.01, 100000),
        "boll_lower": (0, 100000),
    }
    TECH_MIN_ROWS = 20
    TECH_MAX_STALE_DAYS = 30

    FUND_REQUIRED = ["roe_ttm", "gross_margin_ttm", "net_profit_yoy_ttm"]
    FUND_BOUNDS = {
        "roe_ttm": (-100, 100),
        "gross_margin_ttm": (-100, 100),
        "net_profit_yoy_ttm": (None, None),
    }

    VAL_REQUIRED = ["pe_ttm", "pb", "price"]
    VAL_BOUNDS = {
        "pe_ttm": (-1000, 1000),  # 负PE表示亏损，是有效值
        "pb": (0.001, 50),
        "price": (0.01, 50000),
    }
    VAL_MIN_HISTORY = 10

    CAPITAL_REQUIRED = ["north", "margin", "dragon"]
    CAPITAL_MIN_NORTH = 5
    CAPITAL_MIN_MARGIN = 5

    BASIC_REQUIRED_BOOL = True

    def validate_all(self, all_data: Dict[str, Any]) -> QualityReport:
        stock_code = all_data.get("stock_code", "unknown")
        report = QualityReport(
            stock_code=stock_code,
            validation_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        tech_data = all_data.get("tech_data")
        fund_data = all_data.get("fundamental_data") or all_data.get("financial_data")
        val_data = all_data.get("valuation_data")
        capital_data = all_data.get("capital_data")
        basic_info = all_data.get("basic_info")

        dims = {}
        dims["tech"] = self._validate_tech(tech_data)
        dims["fundamental"] = self._validate_fundamental(fund_data)
        dims["valuation"] = self._validate_valuation(val_data)
        dims["capital"] = self._validate_capital(capital_data)
        dims["basic"] = self._validate_basic(basic_info)

        scores = [d.score for d in dims.values()]
        report.overall_score = int(sum(scores) / len(scores)) if scores else 0
        report.overall_grade = _score_to_grade(report.overall_score)
        report.dimensions = dims

        for dim in dims.values():
            for w in dim.warnings:
                report.global_warnings.append(f"[{dim.dimension}] {w}")

        unavailable_dims = [d.dimension for d in dims.values() if d.score == 0]
        if unavailable_dims:
            report.global_warnings.insert(
                0,
                f"数据维度缺失: {', '.join(unavailable_dims)}，相关分析将跳过或基于有限数据推断"
            )

        if report.overall_score < 40:
            report.global_warnings.insert(
                0,
                "CRITICAL: 整体数据质量较差，LLM分析结论的可靠性将大幅下降，建议优先修复数据源"
            )

        return report

    def _validate_tech(self, tech_data: Any) -> DimensionQuality:
        dim = DimensionQuality(dimension="tech")

        if tech_data is None:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append("技术面数据完全缺失(tech_data=None)")
            return dim

        if isinstance(tech_data, dict):
            rows = tech_data if isinstance(tech_data, list) else []
            if not rows and "_data_unavailable" in tech_data:
                dim.score = 0
                dim.grade = "无效"
                dim.failures.append("技术面数据标记为不可用(_data_unavailable)")
                return dim
        elif hasattr(tech_data, "to_dict"):
            rows = tech_data.to_dict("records") if hasattr(tech_data, "to_dict") else []
        elif isinstance(tech_data, list):
            rows = tech_data
        else:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append(f"技术面数据类型异常: {type(tech_data).__name__}")
            return dim

        dim.fields_total = len(self.TECH_REQUIRED)
        dim.fields_pass = 0
        dim.fields_warn = 0
        dim.fields_fail = 0

        row_count = len(rows)
        if row_count < self.TECH_MIN_ROWS:
            dim.warnings.append(
                f"数据行数不足: {row_count}行 < 最低{self.TECH_MIN_ROWS}行,技术指标计算可能不稳定"
            )
            dim.fields_warn += 1
        else:
            dim.fields_pass += 0

        last_row = rows[-1] if rows else {}
        last_date = last_row.get("date", last_row.get("trade_date", ""))
        if last_date:
            try:
                parsed = datetime.strptime(str(last_date)[:10], "%Y-%m-%d")
                stale_days = (datetime.now() - parsed).days
                if stale_days > self.TECH_MAX_STALE_DAYS:
                    dim.warnings.append(
                        f"数据时效性差: 最新日期{last_date},距今{stale_days}天 > {self.TECH_MAX_STALE_DAYS}天阈值"
                    )
                    dim.fields_warn += 1
            except (ValueError, TypeError):
                pass

        for field in self.TECH_REQUIRED:
            has_field = field in last_row
            value = last_row.get(field) if has_field else None
            details = self._check_field(field, value, self.TECH_BOUNDS)
            dim.field_details.append(details)

            if details.status == "fail":
                dim.fields_fail += 1
                dim.failures.append(f"缺失关键字段: {field}")
            elif details.status == "warn":
                dim.fields_warn += 1
                dim.warnings.append(details.issue)
            else:
                dim.fields_pass += 1

        if dim.fields_fail >= len(self.TECH_REQUIRED) * 0.5:
            dim.score = 0
        elif dim.fields_fail > 0:
            dim.score = max(20, 100 - dim.fields_fail * 15 - dim.fields_warn * 3)
        else:
            dim.score = max(60, 100 - dim.fields_warn * 5)

        dim.grade = _score_to_grade(dim.score)
        return dim

    def _validate_fundamental(self, fund_data: Any) -> DimensionQuality:
        dim = DimensionQuality(dimension="fundamental")

        if fund_data is None:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append("基本面数据完全缺失")
            return dim

        if isinstance(fund_data, dict):
            finance = fund_data.get("finance", fund_data.get("financial_data", []))
            if "_data_unavailable" in fund_data:
                dim.score = 0
                dim.grade = "无效"
                dim.failures.append("基本面数据标记为不可用")
                return dim

            if isinstance(finance, list) and len(finance) > 0:
                latest_finance = finance[-1]
            else:
                dim.score = 0
                dim.grade = "无效"
                dim.failures.append("财务数据为空列表")
                return dim

            for field in self.FUND_REQUIRED:
                value = latest_finance.get(field)
                details = self._check_field(field, value, self.FUND_BOUNDS)
                dim.field_details.append(details)

                if details.status == "fail":
                    dim.fields_fail += 1
                    dim.failures.append(f"缺失关键字段: {field}")
                elif details.status == "warn":
                    dim.fields_warn += 1
                    dim.warnings.append(details.issue)
                else:
                    dim.fields_pass += 1

            dim.fields_total = len(self.FUND_REQUIRED)
        else:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append(f"基本面数据类型异常: {type(fund_data).__name__}")
            return dim

        if dim.fields_fail > 0:
            dim.score = max(20, 100 - dim.fields_fail * 30)
        else:
            dim.score = max(60, 100 - dim.fields_warn * 10)

        dim.grade = _score_to_grade(dim.score)
        return dim

    def _validate_valuation(self, val_data: Any) -> DimensionQuality:
        dim = DimensionQuality(dimension="valuation")

        if val_data is None:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append("估值数据完全缺失")
            return dim

        if isinstance(val_data, dict):
            if "_data_unavailable" in val_data:
                dim.score = 0
                dim.grade = "无效"
                dim.failures.append("估值数据标记为不可用")
                return dim

            for field in self.VAL_REQUIRED:
                value = val_data.get(field)
                details = self._check_field(field, value, self.VAL_BOUNDS)
                dim.field_details.append(details)

                if details.status == "fail":
                    dim.fields_fail += 1
                    dim.failures.append(f"缺失或异常: {field}={value}")
                elif details.status == "warn":
                    dim.fields_warn += 1
                    dim.warnings.append(details.issue)
                else:
                    dim.fields_pass += 1

            dim.fields_total = len(self.VAL_REQUIRED)

            pe_history = val_data.get("pe_history", [])
            pb_history = val_data.get("pb_history", [])
            if len(pe_history) < self.VAL_MIN_HISTORY:
                dim.warnings.append(
                    f"PE历史数据不足: {len(pe_history)}条 < {self.VAL_MIN_HISTORY}条,分位数计算不可靠"
                )
                dim.fields_warn += 1
            if len(pb_history) < self.VAL_MIN_HISTORY:
                dim.warnings.append(
                    f"PB历史数据不足: {len(pb_history)}条 < {self.VAL_MIN_HISTORY}条,分位数计算不可靠"
                )
                dim.fields_warn += 1

            pe_ttm = val_data.get("pe_ttm")
            pb = val_data.get("pb")
            if (pe_ttm is not None and pb is not None and
                    isinstance(pe_ttm, (int, float)) and isinstance(pb, (int, float)) and
                    pe_ttm > 0 and pb > 0):
                pe_pb_ratio = pe_ttm / pb
                if pe_pb_ratio > 200:
                    dim.warnings.append(
                        f"PE/PB比值 {pe_pb_ratio:.1f} 极高,数据可能存在异常(盈利率极端或负资产)"
                    )
                    dim.fields_warn += 1
        else:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append(f"估值数据类型异常: {type(val_data).__name__}")
            return dim

        if dim.fields_fail > 0:
            dim.score = max(20, 100 - dim.fields_fail * 30)
        else:
            dim.score = max(60, 100 - dim.fields_warn * 8)

        dim.grade = _score_to_grade(dim.score)
        return dim

    def _validate_capital(self, capital_data: Any) -> DimensionQuality:
        dim = DimensionQuality(dimension="capital")

        if capital_data is None:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append("资金面数据完全缺失")
            return dim

        if isinstance(capital_data, dict):
            if "_data_unavailable" in capital_data:
                dim.score = 0
                dim.grade = "无效"
                dim.failures.append("资金面数据标记为不可用")
                return dim

            north = capital_data.get("north", [])
            margin = capital_data.get("margin", [])
            dragon = capital_data.get("dragon", [])

            dim.fields_total = 3
            dim.fields_pass = 0
            dim.fields_warn = 0
            dim.fields_fail = 0

            if len(north) == 0:
                dim.fields_warn += 1
                dim.warnings.append("北向资金因监管政策自2024-08-16停止披露个股日度持股明细，数据不可用")
            elif len(north) < self.CAPITAL_MIN_NORTH:
                dim.fields_warn += 1
                dim.warnings.append(f"北向资金数据不足: {len(north)}条 < {self.CAPITAL_MIN_NORTH}条（历史数据截止2024-08-16）")
            else:
                dim.fields_pass += 1

            if len(margin) == 0:
                dim.fields_warn += 1
                dim.warnings.append("融资融券数据为空")
            elif len(margin) < self.CAPITAL_MIN_MARGIN:
                dim.fields_warn += 1
                dim.warnings.append(f"融资融券数据不足: {len(margin)}条 < {self.CAPITAL_MIN_MARGIN}条")
            else:
                dim.fields_pass += 1

            if len(dragon) == 0:
                dim.fields_pass += 1
            else:
                dim.fields_pass += 1

            dim.score = max(30, 100 - dim.fields_warn * 20)
        else:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append(f"资金面数据类型异常: {type(capital_data).__name__}")
            return dim

        dim.grade = _score_to_grade(dim.score)
        return dim

    def _validate_basic(self, basic_info: Any) -> DimensionQuality:
        dim = DimensionQuality(dimension="basic")

        if basic_info is None:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append("基本信息完全缺失")
            return dim

        if isinstance(basic_info, dict):
            if "_data_unavailable" in basic_info:
                dim.score = 30
                dim.grade = "较差"
                dim.warnings.append("基本信息标记为不可用")
                return dim

            has_industry = bool(basic_info.get("行业"))
            has_code = bool(basic_info.get("股票代码"))

            dim.fields_total = 2
            dim.fields_pass = int(has_code) + int(has_industry)
            dim.fields_fail = 2 - dim.fields_pass

            if not has_code:
                dim.failures.append("缺失股票代码")
            if not has_industry:
                dim.failures.append("缺失行业分类,将影响行业对标分析")

            if dim.fields_fail >= 2:
                dim.score = 10
            elif dim.fields_fail == 1:
                dim.score = 50
            else:
                dim.score = 90

            dim.grade = _score_to_grade(dim.score)
        else:
            dim.score = 0
            dim.grade = "无效"
            dim.failures.append(f"基本信息类型异常: {type(basic_info).__name__}")

        return dim

    def _check_field(
        self,
        field: str,
        value: Any,
        bounds: Dict[str, Tuple[Optional[float], Optional[float]]],
    ) -> FieldValidation:
        fv = FieldValidation(field=field, value=value, status="pass")

        if value is None:
            fv.status = "fail"
            fv.issue = f"{field}值为空(None)"
            fv.suggestion = "数据源可能未返回该字段,请检查API"
            return fv

        if isinstance(value, float) and (value != value):
            fv.status = "fail"
            fv.issue = f"{field}值为NaN"
            fv.suggestion = "上游计算异常,请检查数据源"
            return fv

        bound = bounds.get(field)
        if bound is not None:
            low, high = bound
            try:
                numeric = float(value)
            except (ValueError, TypeError):
                fv.status = "warn"
                fv.issue = f"{field}={value} 无法转为数值"
                fv.suggestion = "非预期数据类型"
                return fv

            if low is not None and numeric < low:
                fv.status = "warn"
                fv.issue = f"{field}={numeric} 低于合理下限 {low}"
                fv.suggestion = f"可能是计算错误或极端行情,建议交叉验证"
            elif high is not None and numeric > high:
                fv.status = "warn"
                fv.issue = f"{field}={numeric} 超出合理上限 {high}"
                fv.suggestion = f"可能是数据源异常或复权问题"

        return fv


def _score_to_grade(score: int) -> str:
    if score >= 85:
        return "优秀"
    elif score >= 70:
        return "良好"
    elif score >= 55:
        return "一般"
    elif score >= 35:
        return "较差"
    else:
        return "无效"


def _quality_report_to_context(report: QualityReport) -> str:
    dim_names = {
        "tech": "技术面数据", "fundamental": "基本面数据",
        "valuation": "估值面数据", "capital": "资金面数据", "basic": "基本信息",
    }
    lines = []

    lines.append("【数据质量校验报告（DataValidator自动生成，供分析参考）】")
    lines.append(f"整体评分: {report.overall_score}/100 ({report.overall_grade})")
    lines.append(f"校验时间: {report.validation_timestamp}")

    for dim_key, dim in report.dimensions.items():
        label = dim_names.get(dim_key, dim_key)
        status_icon = "[OK]" if dim.score >= 70 else ("[WARN]" if dim.score >= 35 else "[FAIL]")
        lines.append(f"  {status_icon} {label}: {dim.score}/100 ({dim.grade})")

        if dim.failures:
            for f in dim.failures[:2]:
                lines.append(f"      - FAIL: {f}")
        if dim.warnings:
            for w in dim.warnings[:2]:
                lines.append(f"      - WARN: {w}")

    if report.global_warnings:
        lines.append("")
        lines.append("【全局数据警告】")
        for w in report.global_warnings[:5]:
            lines.append(f"  - {w}")

    lines.append("")
    lines.append("【LLM分析指引】")
    if report.overall_score >= 70:
        lines.append("  数据质量良好,可直接基于数据给出结论,适当标注置信度。")
    elif report.overall_score >= 40:
        lines.append("  部分维度的数据质量一般,对缺失/异常维度降低结论置信度,在报告中标注数据限制。")
    else:
        lines.append("  数据质量较差,多个维度存在严重缺失。请勿做任何确定性结论,每个判断前需加「基于有限数据推测」限定。")

    return "\n".join(lines)


validator = DataValidator()

__all__ = [
    "DataValidator",
    "QualityReport",
    "DimensionQuality",
    "FieldValidation",
    "validator",
]