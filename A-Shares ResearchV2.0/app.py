import json
import sys
import logging
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

from layers.agents.chief_agent import ChiefAgent
from db import (
    watchlist_get_all, watchlist_add, watchlist_remove,
    watchlist_update_snapshot, cache_save, cache_get_latest
)
from chat_engine import chat_engine

app = FastAPI()
templates = Jinja2Templates(directory="templates")

reviewer = None


def get_reviewer():
    global reviewer
    if reviewer is None:
        reviewer = ChiefAgent()
    return reviewer


# ==================== 前端页面 ====================

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    watchlist = watchlist_get_all()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "selected_agents": ["tech", "fund", "capital", "industry", "risk", "valuation"],
        "watchlist": watchlist,
    })


@app.post("/generate", response_class=HTMLResponse)
def generate(request: Request, stock_code: str = Form(...),
             agents: List[str] = Form(None)):
    selected_agents = agents if agents else [
        "tech", "fund", "capital", "industry", "risk", "valuation"
    ]
    try:
        reviewer = get_reviewer()
        state = {"selected_agents": selected_agents}
        result = reviewer.analyze(stock_code, state)

        if isinstance(result, dict):
            report = result.get("final_report", "")
            chart_data = result.get("chart_data", {})
            reports = result.get("reports", {})
            overall_grade = result.get("overall_grade", "")
            overall_score = result.get("overall_score", 50)
            context_text = result.get("context_text", "")

            # 保存分析缓存（供对话使用）
            try:
                cache_save(
                    stock_code=stock_code,
                    report_markdown=report,
                    reports=reports,
                    chart_data=chart_data,
                    overall_grade=overall_grade,
                    overall_score=overall_score,
                    context_text=context_text,
                )
            except Exception:
                pass

            # 更新自选股快照
            try:
                price = None
                if chart_data.get("kline") and chart_data["kline"].get("ohlc"):
                    last_ohlc = chart_data["kline"]["ohlc"][-1]
                    price = last_ohlc[1] if len(last_ohlc) > 1 else None
                watchlist_update_snapshot(
                    stock_code=stock_code,
                    last_price=price,
                    last_grade=overall_grade,
                    last_score=overall_score,
                )
            except Exception:
                pass
        else:
            report = result
            chart_data = {}
            overall_grade = ""
            overall_score = 50
    except Exception as e:
        import traceback
        report = f"报告生成错误：{str(e)}\n\n详细信息：{traceback.format_exc()}"
        chart_data = {}
        overall_grade = ""
        overall_score = 50

    watchlist = watchlist_get_all()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stock_code": stock_code,
        "report": report,
        "chart_data_json": json.dumps(chart_data, ensure_ascii=False),
        "overall_grade": overall_grade,
        "overall_score": overall_score,
        "selected_agents": selected_agents,
        "watchlist": watchlist,
    })


# ==================== 自选股 API ====================

@app.get("/api/watchlist")
def api_watchlist_get():
    return JSONResponse(watchlist_get_all())


@app.post("/api/watchlist")
async def api_watchlist_add(req: Request):
    body = await req.json()
    stock_code = body.get("stock_code", "").strip()
    stock_name = body.get("stock_name", "")
    notes = body.get("notes", "")
    if not stock_code:
        return JSONResponse({"error": "股票代码不能为空"}, status_code=400)
    ok = watchlist_add(stock_code, stock_name, notes)
    return JSONResponse({"success": ok, "watchlist": watchlist_get_all()})


@app.delete("/api/watchlist/{stock_code}")
def api_watchlist_remove(stock_code: str):
    ok = watchlist_remove(stock_code)
    return JSONResponse({"success": ok, "watchlist": watchlist_get_all()})


@app.post("/api/watchlist/refresh")
def api_watchlist_refresh():
    """一键刷新所有自选股分析 + 自动拉取研报"""
    wl = watchlist_get_all()
    results = []
    for item in wl:
        code = item["stock_code"]
        try:
            reviewer = get_reviewer()
            result = reviewer.analyze(code)
            if isinstance(result, dict):
                report = result.get("final_report", "")
                chart_data = result.get("chart_data", {})
                reports = result.get("reports", {})
                grade = result.get("overall_grade", "")
                score = result.get("overall_score", 50)
                ctx = result.get("context_text", "")

                cache_save(code, report, reports, chart_data, grade, score, ctx)

                price = None
                if chart_data.get("kline") and chart_data["kline"].get("ohlc"):
                    last = chart_data["kline"]["ohlc"][-1]
                    price = last[1] if len(last) > 1 else None

                watchlist_update_snapshot(code, last_price=price,
                                          last_grade=grade, last_score=score)
                results.append({"code": code, "status": "ok", "grade": grade, "score": score})
            else:
                results.append({"code": code, "status": "ok"})
        except Exception as e:
            results.append({"code": code, "status": "error", "error": str(e)[:100]})

    try:
        from layers.memory.report_fetcher import fetch_reports_for_watchlist
        report_results = fetch_reports_for_watchlist()
        report_count = sum(len(v) for v in report_results.values())
        logging.getLogger("App").info(f"[App] 自选股研报拉取完成: {report_count} 篇")
    except Exception as e:
        logging.getLogger("App").warning(f"[App] 自选股研报拉取失败: {e}")

    return JSONResponse({"results": results, "watchlist": watchlist_get_all()})


# ==================== 分析 API（JSON，供前端AJAX调用） ====================

class AnalyzeRequest(BaseModel):
    stock_code: str
    agents: List[str] = ["tech", "fund", "capital", "industry", "risk", "valuation"]


@app.post("/api/analyze")
def api_analyze(req: AnalyzeRequest):
    """分析股票并返回JSON（含chart_data），供前端异步渲染"""
    try:
        reviewer = get_reviewer()
        state = {"selected_agents": req.agents}
        result = reviewer.analyze(req.stock_code, state)

        if isinstance(result, dict):
            # 保存缓存 + 更新自选股快照
            try:
                cache_save(
                    stock_code=req.stock_code,
                    report_markdown=result.get("final_report", ""),
                    reports=result.get("reports", {}),
                    chart_data=result.get("chart_data", {}),
                    overall_grade=result.get("overall_grade", ""),
                    overall_score=result.get("overall_score", 50),
                    context_text=result.get("context_text", ""),
                )
                price = None
                cd = result.get("chart_data", {})
                if cd.get("kline") and cd["kline"].get("ohlc"):
                    last = cd["kline"]["ohlc"][-1]
                    price = last[1] if len(last) > 1 else None
                watchlist_update_snapshot(
                    stock_code=req.stock_code,
                    last_price=price,
                    last_grade=result.get("overall_grade", ""),
                    last_score=result.get("overall_score", 50),
                )
            except Exception:
                pass

            return JSONResponse({
                "success": True,
                "stock_code": req.stock_code,
                "final_report": result["final_report"],
                "reports": result["reports"],
                "chart_data": result["chart_data"],
                "overall_grade": result["overall_grade"],
                "overall_score": result["overall_score"],
            })
        else:
            return JSONResponse({"success": False, "error": "分析返回格式异常"})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})


# ==================== 对话 API（多模式） ====================

class ChatRequest(BaseModel):
    stock_code: str = ""  # 可选：为空时自动从问题中提取
    messages: List[dict] = []
    new_message: str


@app.post("/api/chat")
def api_chat(req: ChatRequest):
    """多模式对话 - 支持基于报告追问 / 实时数据问答 / 纯知识问答"""
    if not req.new_message:
        return JSONResponse({"error": "缺少消息内容"}, status_code=400)

    reply = chat_engine.chat(
        stock_code=req.stock_code,
        messages=req.messages,
        new_message=req.new_message,
    )
    return JSONResponse({"reply": reply, "stock_code": req.stock_code})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)