import re

def clean_rows(s):
    if not isinstance(s, str):
        return s
    s = re.sub(r'"', '', s)
    s = re.sub(r'{', '', s)
    s = re.sub(r'}', '', s)
    s = re.sub(r'[\[\]]', '', s)
    s = re.sub(r"'", '', s)
    return s

def initial_clean(s):
    s = " ".join(s.split())
    s = re.sub(r'n/', '', s)
    return s