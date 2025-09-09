import re

ISO_PATTERN = re.compile(r"^\s*\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?\s*$")

def sanitize_sheet_name(name: str) -> str:
    bad = '[]:*?/\\'
    for ch in bad: name = name.replace(ch, ' ')
    return name[:31]
