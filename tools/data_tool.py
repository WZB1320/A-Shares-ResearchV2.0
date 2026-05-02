# tools/data_tool.py
import akshare as ak

def get_stock_data(code):
    """
    纯数据获取函数：输入股票代码，返回真实A股数据字典
    和LangGraph流程完全解耦，可以在任何地方复用

    【数据源切换说明】
    如需更换为 Tushare / 东财 / 同花顺等数据源：
    1. 替换下面所有 akshare 代码
    2. 保持 return 的字段名称完全不变
    3. 主程序、Agent、前端页面均无需任何修改
    """
    try:
        # 1. 实时行情
        spot = ak.stock_zh_a_spot_em(symbol=code)
        price = round(float(spot.iloc[0]["最新价"]), 2)

        # 2. 财务信息（真实）
        fina = ak.stock_financial_report_emu(symbol=code)
        roe = fina.iloc[-1]["净资产收益率(%)"]
        gross_profit = fina.iloc[-1]["销售毛利率(%)"]
        net_profit = fina.iloc[-1]["净利润同比增长率(%)"]

        # 3. 估值数据（真实 PE/PB）
        val = ak.stock_a_pe_pb(symbol=code)
        pe_ttm = round(val["pe"].iloc[-1], 2)
        pb = round(val["pb"].iloc[-1], 2)

        # 4. 10 年估值历史（真实）
        pe_history = val["pe"].dropna().tail(120).tolist()
        pb_history = val["pb"].dropna().tail(120).tolist()
        pe_avg = round(sum(pe_history)/len(pe_history), 2)

    except Exception as e:
        price = 0
        pe_ttm = 0
        pb = 0
        pe_history = []
        pb_history = []
        pe_avg = 0
        roe = 0
        gross_profit = 0
        net_profit = "数据获取异常"

    return {
        "price": price,
        "pe_ttm": pe_ttm,
        "pb": pb,
        "pe_history": pe_history,
        "pb_history": pb_history,
        "pe_10_avg": pe_avg,
        "roe": round(roe, 2),
        "gross_profit": round(gross_profit, 2),
        "net_profit": net_profit
    }