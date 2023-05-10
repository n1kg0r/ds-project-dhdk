import re

def remove_special_chars(s: str) -> str:
    return re.sub(r'[^\w\s/]+', '', s)

def clean_string(s: str) -> str:
    return "'" + s.replace('"', '') + "'"