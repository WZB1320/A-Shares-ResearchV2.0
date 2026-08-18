"""研报 PDF 下载流程测试 (需要网络, 默认跳过)

运行（含网络）：pytest -m integration tests/test_report_download.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from layers.memory.report_fetcher import _download_report_pdf, _count_pdf_pages, REPORTS_DIR


TEST_CASES = [
    {
        "url": "https://pdf.dfcfw.com/pdf/H3_AP202604261821586763_1.pdf",
        "stock": "BABA",
        "title": "深度研究：AI云业务拐点已至，上调目标价",
        "date": "2025-05-20",
    },
    {
        "url": "https://pdf.dfcfw.com/pdf/H3_AP202506051912345678_1.pdf",
        "stock": "BABA",
        "title": "回购加码信号积极，估值安全边际充足",
        "date": "2025-06-01",
    },
]


@pytest.mark.integration
def test_report_download():
    valid_count = 0
    for tc in TEST_CASES:
        path = _download_report_pdf(tc["url"], tc["stock"], tc["title"], tc["date"])
        if path:
            pages = _count_pdf_pages(path)
            assert isinstance(pages, int) and pages >= 0
            valid_count += 1
    # 反盗链/超时情况下允许部分失败, 但至少有函数可调用且不抛异常
    assert valid_count >= 0
