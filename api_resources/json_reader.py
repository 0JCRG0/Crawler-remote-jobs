import json
import requests
import pretty_errors

#Request the api...
url = 'https://remoteok.com/api'

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    
    test = data[:15]
    #test_5 = jobs[:9]
    #print just to test
    """
    pretty_json = json.dumps(data, indent=4)
    print(pretty_json, type(data))
    """
    pretty_json = json.dumps(test, indent=4)
    print(pretty_json, type(test))
    