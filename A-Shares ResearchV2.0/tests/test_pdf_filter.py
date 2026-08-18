"""PDF 页码检测 + 研报标题筛选

离线部分（默认运行）：研报标题深度筛选。
网络部分（默认跳过）：真实 PDF 下载与页码检测。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import requests
from layers.memory.report_fetcher import _count_pdf_pages, _is_depth_report

PDF_URL = "https://pdf.dfcfw.com/pdf/H3_AP202604261821586763_1.pdf"


def test_is_depth_report():
    tests = [
        ("深度研究：AI云业务拐点已至", True),
        ("首次覆盖：某某公司投资价值分析", True),
        ("晨会纪要2025-05-20", False),
        ("日报：市场点评", False),
        ("事件点评：回购公告", False),
        ("专题研究：半导体行业", True),
    ]
    for t, expected in tests:
        assert _is_depth_report(t) == expected, f"《{t}》 期望 {expected}"


@pytest.mark.integration
def test_pdf_page_count():
    resp = requests.get(PDF_URL, timeout=30)
    assert resp.status_code == 200, f"PDF 下载失败: {resp.status_code}"
    pages = _count_pdf_pages(resp.content)
    assert isinstance(pages, int) and pages >= 0
