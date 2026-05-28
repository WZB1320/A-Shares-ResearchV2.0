"""调试 PDF 页码检测"""
import sys, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

pdf_url = "https://pdf.dfcfw.com/pdf/H3_AP202604261821586763_1.pdf"
resp = requests.get(pdf_url, timeout=30)
content = resp.content

print(f"PDF大小: {len(content)} bytes")
print(f"前200字节: {content[:200]}")

text = content.decode("latin-1", errors="ignore")

type_page_count = len(re.findall(r"/Type\s*/Page\b", text))
type_pages_count = len(re.findall(r"/Type\s*/Pages\b", text))
print(f"\n/Type/Page: {type_page_count}")
print(f"/Type/Pages: {type_pages_count}")

for m in re.finditer(r"/Type\s*/Page", text):
    ctx = text[m.start():m.start()+200]
    print(f"  匹配: ...{repr(ctx[:100])}...")

for i in re.finditer(r"/Count\s+(\d+)", text):
    print(f"\n/Count: {i.group(1)}")

for i in re.finditer(r"/Pages\s*<<.*?/Count\s+(\d+)", text, re.DOTALL):
    print(f"\n/Pages/Count: {i.group(1)}")