"""
图表数据构建器 - 从原始数据 + Skill输出中提取前端ECharts所需JSON
"""
import logging
import math
from typing import Dict, List, Optional, Any

logger = logging.getLogger("ChartBuilder")

# ECharts K线图最多展示的数据点数量（避免前端卡顿）
MAX_KLINE_POINTS = 250
# 估值分位图保留的历史数据点
MAX_VALUATION_POINTS = 200


class ChartBuilder:

    def build(self, stock_code: str, all_data: Dict, agent_reports: Dict = None) -> Dict:
        """
        构建所有图表数据
        all_data: DataConnector.fetch_all() 返回的原始数据
        agent_reports: 各Agent分析结果（用于雷达图评分）
        """
        chart_data = {}

        tech_data = all_data.get("tech_data")
        if tech_data is not None and hasattr(tech_data, 'empty') and not tech_data.empty:
            try:
                chart_data.update(self._build_kline(tech_data))
                chart_data.update(self._build_indicators(tech_data))
            except Exception as e:
                logger.warning(f"[ChartBuilder] K线/指标数据构建失败: {e}")

        capital_data = all_data.get("capital_data")
        if capital_data:
            try:
                chart_data["capital_flow"] = self._build_capital_flow(capital_data)
            except Exception as e:
                logger.warning(f"[ChartBuilder] 资金流数据构建失败: {e}")

        valuation_data = all_data.get("valuation_data")
        fundamental_data = all_data.get("fundamental_data") or all_data.get("financial_data")
        if valuation_data:
            try:
                chart_data["valuation_gauge"] = self._build_valuation_gauge(
                    stock_code, valuation_data, fundamental_data
                )
            except Exception as e:
                logger.warning(f"[ChartBuilder] 估值仪表数据构建失败: {e}")

        if fundamental_data:
            try:
                chart_data["dupont"] = self._build_dupont(fundamental_data)
            except Exception as e:
                logger.warning(f"[ChartBuilder] 杜邦数据构建失败: {e}")

        if agent_reports:
            try:
                chart_data["radar"] = self._build_radar(agent_reports)
            except Exception as e:
                logger.warning(f"[ChartBuilder] 雷达图数据构建失败: {e}")

        logger.info(f"[ChartBuilder] 图表数据构建完成: {stock_code} | 类型数: {len(chart_data)}")
        return chart_data

    # ==================== K线图 ====================

    def _build_kline(self, df) -> Dict:
        df_tail = df.tail(MAX_KLINE_POINTS)
        dates = df_tail["date"].astype(str).tolist()

        ohlc = []
        for _, row in df_tail.iterrows():
            ohlc.append([
                self._f(row.get("open")),
                self._f(row.get("close")),
                self._f(row.get("low")),
                self._f(row.get("high")),
            ])

        ma_lines = {}
        for period in [5, 10, 20, 60]:
            col = f"ma{period}"
            if col in df_tail.columns:
                ma_lines[f"ma{period}"] = [self._f(v) for v in df_tail[col].tolist()]

        volumes = [int(v) if v and not math.isnan(v) else 0 for v in df_tail["volume"].tolist()]

        return {
            "kline": {
                "dates": dates,
                "ohlc": ohlc,
                "mas": ma_lines,
                "volumes": volumes,
            }
        }

    # ==================== 技术指标子图 ====================

    def _build_indicators(self, df) -> Dict:
        df_tail = df.tail(MAX_KLINE_POINTS)
        dates = df_tail["date"].astype(str).tolist()

        result = {"indicators": {"dates": dates}}

        # MACD
        if all(c in df_tail.columns for c in ["macd", "signal", "macd_hist"]):
            result["indicators"]["macd"] = {
                "dif": [self._f(v) for v in df_tail["macd"].tolist()],
                "dea": [self._f(v) for v in df_tail["signal"].tolist()],
                "hist": [self._f(v) for v in df_tail["macd_hist"].tolist()],
            }

        # KDJ
        if all(c in df_tail.columns for c in ["k", "d", "j"]):
            result["indicators"]["kdj"] = {
                "k": [self._f(v) for v in df_tail["k"].tolist()],
                "d": [self._f(v) for v in df_tail["d"].tolist()],
                "j": [self._f(v) for v in df_tail["j"].tolist()],
            }

        # RSI
        if "rsi6" in df_tail.columns:
            result["indicators"]["rsi"] = [self._f(v) for v in df_tail["rsi6"].tolist()]

        # 布林带
        if all(c in df_tail.columns for c in ["close", "boll_upper", "boll_mid", "boll_lower"]):
            result["indicators"]["boll"] = {
                "close": [self._f(v) for v in df_tail["close"].tolist()],
                "upper": [self._f(v) for v in df_tail["boll_upper"].tolist()],
                "mid": [self._f(v) for v in df_tail["boll_mid"].tolist()],
                "lower": [self._f(v) for v in df_tail["boll_lower"].tolist()],
            }

        return result

    # ==================== 资金流向 ====================

    def _build_capital_flow(self, capital_data: Dict) -> Dict:
        result = {}

        # 北向资金
        north = capital_data.get("north", [])
        if north and isinstance(north, list) and len(north) > 0:
            result["north"] = self._extract_flow_series(north, max_points=60)

        # 融资融券
        margin = capital_data.get("margin", [])
        if margin and isinstance(margin, list) and len(margin) > 0:
            result["margin"] = self._extract_flow_series(margin, max_points=60, field_map={
                "net_inflow": "net_amount", "date": "trade_date"
            })

        return result

    def _extract_flow_series(self, data: List, max_points: int = 60,
                              field_map: Dict = None) -> Dict:
        if field_map is None:
            field_map = {"net_inflow": "net_flow", "date": "trade_date"}

        data = data[-max_points:]
        dates = []
        values = []

        for item in data:
            if isinstance(item, dict):
                d = item.get(field_map["date"], "")
                v = item.get(field_map["net_inflow"])
                if d:
                    dates.append(str(d))
                    values.append(self._f(v))

        return {"dates": dates, "values": values}

    # ==================== 估值仪表盘 ====================

    def _build_valuation_gauge(self, stock_code: str, valuation_data: Dict,
                                fundamental_data: Dict = None) -> Dict:
        from layers.skills.valuation_skill import ValuationSkill, valuation_skill

        try:
            signals = valuation_skill.analyze(valuation_data, fundamental_data)
        except Exception:
            return {"error": "估值数据不足"}

        m = signals.metrics
        result = {
            "pe_ttm": self._f(m.pe_ttm),
            "pb": self._f(m.pb),
            "pe_percentile": self._f(m.pe_percentile),
            "pb_percentile": self._f(m.pb_percentile),
            "pe_grade": self._percentile_to_grade(m.pe_percentile),
            "pb_grade": self._percentile_to_grade(m.pb_percentile),
            "overall_grade": signals.valuation_grade,
            "overall_score": signals.overall_score,
            "category": m.industry_category or "通用",
            "thresholds": {
                "pe_low": m.percentile_pe_low_threshold,
                "pe_high": m.percentile_pe_high_threshold,
                "pb_low": m.percentile_pb_low_threshold,
                "pb_high": m.percentile_pb_high_threshold,
            }
        }

        # PE历史序列（如果有）
        pe_history = valuation_data.get("pe_history") or valuation_data.get("history", [])
        if isinstance(pe_history, list) and len(pe_history) > 0:
            result["pe_history"] = self._extract_valuation_history(pe_history, "pe_ttm")

        pb_history = valuation_data.get("pb_history", [])
        if isinstance(pb_history, list) and len(pb_history) > 0:
            result["pb_history"] = self._extract_valuation_history(pb_history, "pb")

        return result

    def _percentile_to_grade(self, pct) -> str:
        if pct is None:
            return "未知"
        if pct < 20:
            return "低估"
        elif pct < 40:
            return "合理偏低"
        elif pct < 60:
            return "合理"
        elif pct < 80:
            return "合理偏高"
        else:
            return "高估"

    def _extract_valuation_history(self, history: List, field: str,
                                    max_points: int = MAX_VALUATION_POINTS) -> Dict:
        data = history[-max_points:]
        dates = []
        values = []
        for item in data:
            if isinstance(item, dict):
                dates.append(str(item.get("trade_date", item.get("date", ""))))
                values.append(self._f(item.get(field)))
        return {"dates": dates, "values": values}

    # ==================== 杜邦分析 ====================

    def _build_dupont(self, fundamental_data: Dict) -> Dict:
        from layers.skills.fund_skill import FundSkill, fund_skill

        try:
            signals = fund_skill.analyze(fundamental_data)
        except Exception:
            return {"error": "基本面数据不足"}

        prof = signals.profitability
        dup = prof.dupont

        return {
            "roe": self._f(dup.roe),
            "net_margin": self._f(dup.net_margin),
            "asset_turnover": self._f(dup.asset_turnover),
            "equity_multiplier": self._f(dup.equity_multiplier),
            "contribution": dup.roe_contribution,
            "profit_quality": prof.profit_quality.value if hasattr(prof.profit_quality, 'value') else str(prof.profit_quality),
            "gross_margin": self._f(prof.gross_margin),
            "net_margin_full": self._f(prof.net_margin),
            "roe_ttm": self._f(prof.roe_ttm),
            "roa_ttm": self._f(prof.roa_ttm),
            "growth": {
                "revenue_yoy": self._f(signals.growth.revenue_growth_yoy),
                "profit_yoy": self._f(signals.growth.profit_growth_yoy),
                "stage": signals.growth.growth_stage,
            },
            "balance": {
                "debt_to_asset": self._f(signals.balance_sheet.debt_to_asset),
                "current_ratio": self._f(signals.balance_sheet.current_ratio),
            }
        }

    # ==================== 雷达图 ====================

    def _build_radar(self, agent_reports: Dict) -> Dict:
        """
        从各Agent报告中提取评分，构建雷达图
        agent_reports: {"tech": AgentReport_dict, "fund": ..., "capital": ..., ...}
        """
        dim_names = {
            "tech": "技术面",
            "fund": "基本面",
            "capital": "资金面",
            "industry": "行业面",
            "valuation": "估值面",
            "risk": "风险面",
        }

        indicators = []
        scores = []
        grades = []

        for key, name in dim_names.items():
            report = agent_reports.get(key, {})
            if isinstance(report, dict):
                score = report.get("overall_score", 50)
                grade = report.get("grade", "中性")
            else:
                score = 50
                grade = "中性"
            indicators.append({"name": name, "max": 100})
            scores.append(score)
            grades.append(grade)

        return {
            "indicators": indicators,
            "scores": scores,
            "grades": grades,
        }

    @staticmethod
    def _f(val, default=0.0):
        """安全浮点数转换"""
        if val is None:
            return default
        try:
            v = float(val)
            if math.isnan(v) or math.isinf(v):
                return default
            return round(v, 2)
        except (TypeError, ValueError):
            return default


chart_builder = ChartBuilder()