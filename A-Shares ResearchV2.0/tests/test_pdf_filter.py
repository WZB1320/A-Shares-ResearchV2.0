"""测试 PDF 页码检测 + 研报标题筛选"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.report_fetcher import _count_pdf_pages, _is_depth_report
import requests

pdf_url = "https://pdf.dfcfw.com/pdf/H3_AP202604261821586763_1.pdf"
try:
    resp = requests.get(pdf_url, timeout=30)
    pages = _count_pdf_pages(resp.content)
    print(f"PDF: {pages} 页")
except Exception as e:
    print(f"PDF下载失败: {e}")

tests = [
    "深度研究：AI云业务拐点已至",
    "首次覆盖：某某公司投资价值分析",
    "晨会纪要2025-05-20",
    "日报：市场点评",
    "事件点评：回购公告",
    "专题研究：半导体行业",
]
for t in tests:
    print(f'  "{t[:30]}" → 深度: {_is_depth_report(t)}')