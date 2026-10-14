from pathlib import Path
import re

for file in Path(".").rglob("*.py"):
    try:
        text = file.read_text(encoding="utf-8")
    except:
        continue

    old = text

    # 只处理 help="..."
    text = re.sub(
        r'(help\s*=\s*"[^"]*?)(?<!%)%(?!%)([^"]*")',
        r'\1%%\2',
        text
    )

    if text != old:
        file.write_text(text, encoding="utf-8")
        print("fixed:", file)
