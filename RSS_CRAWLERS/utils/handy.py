import re

def clean_link_rss(s):
    # Remove leading/trailing white space
    s = s.strip()
        
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
        
    # Remove newline characters
    s = re.sub(r'\n', '', s)
        
    return s


def clean_other_rss(s):
    # Remove leading/trailing white space
    s = s.strip()
        
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
        
    # Remove newline characters
    s = re.sub(r'\n', '', s)
        
    # Remove HTML tags
    s = re.sub(r'<.*?>', '', s)
        
    # Remove non-alphanumeric characters (except for spaces)
    s = re.sub(r'[^a-zA-Z0-9\s]+', '', s)
        
    # Remove symbols
    s = re.sub(r'[-–—•@Ôªø]+', '', s)
            
    return s