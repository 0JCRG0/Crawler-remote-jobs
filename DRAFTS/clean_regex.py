import re


def bye_regex(s):
    # Remove leading/trailing white space
    s = s.strip()
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    # Remove newline characters
    s = re.sub(r'\n', '', s)
    # Replace word
    s = re.sub(r'Posted', '', s)
    # Remove HTML tags
    s = re.sub(r'<.*?>', ' ', s)
    return s

def pandas_regex(s):
    if not isinstance(s, str):
        return s
    #Replace regex for ú
    s = re.sub(r'√∫', 'ú', s)
    #Replace regex for ó
    s = re.sub(r'√≥', 'ó', s)
    #Replace regex for é
    s = re.sub(r'√©', 'é', s)
    #Replace regex for á
    s = re.sub(r'√°', 'á', s)
    # Replace regex for í
    s = re.sub(r'√≠', 'í', s)
    #Replace regex for É
    s = re.sub(r'√â', 'É', s)
    #Replace regex for Ó
    s = re.sub(r'√ì', 'Ó', s)
    #Replace regex for Í
    s = re.sub(r'√ç', 'Í', s)
    #Replace regex for Á
    s = re.sub(r'√Å', 'Á', s)
    #Replace regex for Ú
    s = re.sub(r'√ö', 'Ú', s)
    return s