from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sys
sys.path.append(".")

# 导入ChiefReviewerAgent
from agents.chief_reviewer import ChiefReviewerAgent

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# 延迟初始化ChiefReviewerAgent（避免启动时出错）
reviewer = None

def get_reviewer():
    global reviewer
    if reviewer is None:
        reviewer = ChiefReviewerAgent()
    return reviewer

# 首页：显示输入框
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# 生成报告
@app.post("/generate", response_class=HTMLResponse)
def generate(request: Request, stock_code: str = Form(...)):
    try:
        reviewer = get_reviewer()
        result = reviewer.analyze(stock_code)
        report = result.get("final_report", "生成失败")
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        report = f"报告生成错误：{str(e)}\n\n详细信息：{error_detail}"

    return templates.TemplateResponse("index.html", {
        "request": request,
        "stock_code": stock_code,
        "report": report
    })