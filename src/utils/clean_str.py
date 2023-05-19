def remove_special_chars(s: str) -> str:
    if '\"' in s:
        return s.replace('\"', '\\\\\\"')
    elif '"' in s:
        return s.replace('"', '\\\\\\"')
    else:
        return s
    
