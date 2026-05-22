import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.validators import DataValidator, QualityReport, DimensionQuality, FieldValidation, validator
from layers.agents.chief_agent import ChiefAgent


def build_healthy_data():
    return {
        "stock_code": "600519",
        "basic_info": {
            "股票代码": "600519",
            "行业": "白酒",
            "来源": "local_api",
        },
        "tech_data": [
            {
                "date": "2024-06-15", "close": 1680.5, "volume": 5800000,
                "open": 1670.0, "high": 1690.0, "low": 1665.0,
                "ma5": 1675.0, "ma10": 1660.0, "ma20": 1640.0, "ma60": 1580.0,
                "macd": 12.5, "signal": 10.2, "macd_hist": 2.3,
                "rsi6": 62.0, "k": 58.0, "d": 52.0, "j": 70.0,
                "pct_change": 1.5, "turnover": 0.45,
                "boll_upper": 1720.0, "boll_lower": 1620.0, "boll_mid": 1640.0,
                "ema12": 1660.0, "ema26": 1630.0, "rsv": 60.0,
            },
            {
                "date": "2024-06-14", "close": 1655.0, "volume": 4200000,
                "open": 1650.0, "high": 1665.0, "low": 1645.0,
                "ma5": 1650.0, "ma10": 1650.0, "ma20": 1635.0, "ma60": 1575.0,
                "macd": 10.2, "signal": 9.0, "macd_hist": 1.2,
                "rsi6": 55.0, "k": 52.0, "d": 48.0, "j": 58.0,
                "pct_change": 0.8, "turnover": 0.35,
                "boll_upper": 1710.0, "boll_lower": 1610.0, "boll_mid": 1635.0,
                "ema12": 1655.0, "ema26": 1625.0, "rsv": 55.0,
            },
        ],
        "fundamental_data": {
            "finance": [
                {"roe": 25.5, "gross_profit": 92.0, "net_profit": "增长15%"},
            ],
            "basic_info": {"行业": "白酒"},
        },
        "valuation_data": {
            "price": 1680.5,
            "pe_ttm": 32.5,
            "pb": 12.8,
            "pe_history": [28, 30, 31, 29, 33, 32, 35, 30, 28, 31, 33, 32],
            "pb_history": [10, 11, 12, 11, 13, 12, 14, 11, 10, 12, 13, 12],
        },
        "capital_data": {
            "north": [{"date": "2024-06-15", "net_flow": 5000}],
            "margin": [{"date": "2024-06-15", "balance": 120000}],
            "dragon": [],
        },
        "financial_data": {
            "roe": 25.5, "gross_profit": 92.0, "net_profit": "增长15%",
        },
    }


def build_bad_data():
    return {
        "stock_code": "000001",
        "basic_info": {},
        "tech_data": None,
        "fundamental_data": {"_data_unavailable": True},
        "valuation_data": {
            "_data_unavailable": True,
            "price": None,
            "pe_ttm": None,
            "pb": None,
            "pe_history": [],
            "pb_history": [],
        },
        "capital_data": {"north": [], "margin": [], "dragon": [], "_data_unavailable": True},
        "financial_data": {"roe": None, "gross_profit": None, "net_profit": None},
    }


def build_partial_data():
    return {
        "stock_code": "000858",
        "basic_info": {"股票代码": "000858", "行业": "酒"},
        "tech_data": [
            {
                "date": "2024-06-15", "close": 152.0, "volume": 25000000,
                "ma5": 150.0, "ma20": 148.0, "macd": -0.5,
                "rsi6": 35.0,
            },
        ],
        "fundamental_data": {
            "finance": [
                {"roe": 18.0, "gross_profit": None, "net_profit": "增长8%"},
            ],
        },
        "valuation_data": {
            "price": 152.0, "pe_ttm": 18.5, "pb": 4.2,
            "pe_history": [16, 17, 18, 19],
            "pb_history": [3.5, 3.8, 4.0],
        },
        "capital_data": {"north": [], "margin": [], "dragon": []},
        "financial_data": {"roe": 18.0, "gross_profit": None, "net_profit": "增长8%"},
    }


def build_edge_case_data():
    return {
        "stock_code": "300750",
        "basic_info": {"股票代码": "300750"},
        "tech_data": [
            {
                "date": "2024-06-15", "close": -10.0, "volume": 0,
                "ma5": float("nan"), "ma20": -50.0, "macd": 500.0,
                "rsi6": 999.0, "k": 500.0, "j": 999.0,
            },
        ],
        "fundamental_data": {
            "finance": [
                {"roe": -500.0, "gross_profit": 200.0, "net_profit": "暴增5000%"},
            ],
        },
        "valuation_data": {
            "price": -10.0, "pe_ttm": -50.0, "pb": 0.001,
            "pe_history": [1, 2],
            "pb_history": [],
        },
        "capital_data": None,
        "financial_data": {"roe": -500.0, "gross_profit": 200.0, "net_profit": "暴增5000%"},
    }


def test_validator_standalone():
    v = DataValidator()

    print("=" * 60)
    print("  DataValidator 独立验证测试")
    print("=" * 60)

    print("\n--- 1. 健康数据 ---")
    report = v.validate_all(build_healthy_data())
    print(f"  整体评分: {report.overall_score}/100 ({report.overall_grade})")
    for dim, dq in report.dimensions.items():
        print(f"    {dim}: {dq.score}/100 ({dq.grade}) | pass={dq.fields_pass} warn={dq.fields_warn} fail={dq.fields_fail}")
    assert report.overall_score >= 70, f"健康数据评分应>=70,实际={report.overall_score}"
    print("  [OK] 健康数据检验通过")

    print("\n--- 2. 严重缺失数据 ---")
    report = v.validate_all(build_bad_data())
    print(f"  整体评分: {report.overall_score}/100 ({report.overall_grade})")
    assert report.overall_score < 40, f"缺失数据评分应<40,实际={report.overall_score}"
    assert report.has_critical_failures(), "应该有严重问题标记"
    for dim, dq in report.dimensions.items():
        if dq.score == 0:
            print(f"    [FAIL] {dim}: {dq.summary if hasattr(dq, 'summary') else dq.grade}")
    print("  [OK] 缺失数据正确拦截")

    print("\n--- 3. 部分缺失数据 ---")
    report = v.validate_all(build_partial_data())
    print(f"  整体评分: {report.overall_score}/100 ({report.overall_grade})")
    print(f"  警告数: {len(report.global_warnings)}")
    assert report.overall_score >= 30, f"部分缺失数据评分应>=30,实际={report.overall_score}"
    print("  [OK] 部分缺失数据宽容处理")

    print("\n--- 4. 边界异常值数据 ---")
    report = v.validate_all(build_edge_case_data())
    print(f"  整体评分: {report.overall_score}/100 ({report.overall_grade})")
    for dim, dq in report.dimensions.items():
        if dq.score == 0:
            print(f"    [FAIL] {dim}: {dq.grade}")
        for fv in dq.field_details:
            if fv.status == "warn":
                print(f"    [WARN] {fv.field} = {fv.value}: {fv.issue}")
    assert report.overall_score < 60, f"边界数据评分应<60,实际={report.overall_score}"
    print("  [OK] 异常值检测正常触发")

    print("\n--- 5. QualityReport to_context_string ---")
    report = v.validate_all(build_partial_data())
    ctx = report.to_context_string()
    lines = ctx.split("\n")
    print(f"  生成上下文 {len(lines)} 行")
    assert "数据质量校验报告" in ctx, "应包含报告标题"
    assert "LLM分析指引" in ctx, "应包含LLM指引"
    assert "WARN" in ctx or "[OK]" in ctx, "应包含维度状态标记"
    print("  [OK] 上下文生成正确")

    print("\n--- 6. QualityReport to_dict ---")
    d = report.to_dict()
    assert "overall_score" in d, "应有overall_score"
    assert "dimensions" in d, "应有dimensions"
    assert "global_warnings" in d, "应有global_warnings"
    print("  [OK] 序列化正常")

    print(f"\n[PASS] 全部6项独立测试通过")


def test_chief_agent_integration():
    print("\n" + "=" * 60)
    print("  ChiefAgent 集成验证测试")
    print("=" * 60)

    print("\n--- 7. ChiefAgent 导入验证 ---")
    ca = ChiefAgent()
    assert hasattr(ca, "analyze"), "ChiefAgent应有analyze方法"
    print("  [OK] ChiefAgent 正常实例化")

    print("\n--- 8. quality_context 传递验证 ---")
    from layers.agents.tech_agent import TechAgent
    agent = TechAgent()
    import inspect
    src = inspect.getsource(agent.analyze)
    assert "quality_context" in src, "TechAgent应处理quality_context"
    print("  [OK] TechAgent 已集成 quality_context")

    from layers.agents.fund_agent import FundAgent
    src = inspect.getsource(FundAgent().analyze)
    assert "quality_context" in src, "FundAgent应处理quality_context"
    print("  [OK] FundAgent 已集成 quality_context")

    from layers.agents.valuation_agent import ValuationAgent
    src = inspect.getsource(ValuationAgent().analyze)
    assert "quality_context" in src, "ValuationAgent应处理quality_context"
    print("  [OK] ValuationAgent 已集成 quality_context")

    from layers.agents.capital_agent import CapitalAgent
    src = inspect.getsource(CapitalAgent().analyze)
    assert "quality_context" in src, "CapitalAgent应处理quality_context"
    print("  [OK] CapitalAgent 已集成 quality_context")

    from layers.agents.industry_agent import IndustryAgent
    src = inspect.getsource(IndustryAgent().analyze)
    assert "quality_context" in src, "IndustryAgent应处理quality_context"
    print("  [OK] IndustryAgent 已集成 quality_context")

    from layers.agents.risk_agent import RiskAgent
    src = inspect.getsource(RiskAgent().analyze)
    assert "quality_context" in src, "RiskAgent应处理quality_context"
    print("  [OK] RiskAgent 已集成 quality_context")

    print(f"\n[PASS] 全部8项测试通过")


def test_layers_import():
    print("\n--- 9. 包导出验证 ---")
    from layers import DataValidator, QualityReport, DimensionQuality, FieldValidation, validator
    assert hasattr(validator, "validate_all"), "validator应有validate_all"
    assert callable(validator.validate_all), "validate_all应可调用"
    print("  [OK] 包导出路径正常")

    print(f"\n[PASS] 全部9项测试通过")
    print("=" * 60)
    print("  数据质量层 (DataValidator) 实现完成")
    print("=" * 60)


if __name__ == "__main__":
    test_validator_standalone()
    test_chief_agent_integration()
    test_layers_import()