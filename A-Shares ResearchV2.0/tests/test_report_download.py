"""测试研报PDF下载流程"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from layers.memory.report_fetcher import (
    _download_report_pdf, _count_pdf_pages, REPORTS_DIR
)

print(f"报告存储目录: {REPORTS_DIR}")
print()

test_cases = [
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

valid_count = 0
for tc in test_cases:
    print(f"下载: {tc['title'][:40]}...")
    path = _download_report_pdf(tc["url"], tc["stock"], tc["title"], tc["date"])
    if path:
        pages = _count_pdf_pages(path)
        size_kb = Path(path).stat().st_size / 1024
        print(f"  成功: {Path(path).name} | {size_kb:.0f}KB | {pages}页")
        valid_count += 1
    else:
        print(f"  失败（反盗链/超时）")
    print()

print(f"\n下载成功: {valid_count}/{len(test_cases)}")

import os
files = list(REPORTS_DIR.glob("*.pdf")) if REPORTS_DIR.exists() else []
print(f"\nreports/ 目录文件数: {len(files)}")
for f in files:
    print(f"  {f.name} ({f.stat().st_size / 1024:.0f}KB)")