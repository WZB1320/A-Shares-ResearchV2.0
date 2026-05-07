from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List
import sys
sys.path.append(".")

from layers.agents.chief_agent import ChiefAgent

app = FastAPI()
templates = Jinja2Templates(directory="templates")

reviewer = None

def get_reviewer():
    global reviewer
    if reviewer is None:
        reviewer = ChiefAgent()
    return reviewer

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/generate", response_class=HTMLResponse)
def generate(request: Request, stock_code: str = Form(...), agents: List[str] = Form(None)):
    try:
        reviewer = get_reviewer()
        # 如果没有选择Agent，默认全选
        selected_agents = agents if agents else ["tech", "fund", "capital", "industry", "risk", "valuation"]
        result = reviewer.analyze(stock_code, selected_agents=selected_agents)
        report = result.get("final_report", "生成失败")
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        report = f"报告生成错误：{str(e)}\n\n详细信息：{error_detail}"

    return templates.TemplateResponse("index.html", {
        "request": request,
        "stock_code": stock_code,
        "report": report,
        "selected_agents": selected_agents
    })
