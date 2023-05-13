import re

def remove_special_chars(s: str) -> str:
    if '\"' in s:
        return s.replace('\"', '\\\"')
    elif '"' in s:
        return s.replace('"', '\\\"')
    else:
        return s
    # return re.sub(r'[^\w\s/]+', '', s)

def clean_string(s: str) -> str:
    return "'" + s.replace('"', '') + "'"