import json
import requests

#Request the api...
url = 'https://4dayweek.io/api'

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    
    jobs = data['jobs']
    test_5 = jobs[:9]
    #print just to test
    """
    pretty_json = json.dumps(data, indent=4)
    print(pretty_json, type(data))
    """
    pretty_json = json.dumps(test_5, indent=4)
    print(pretty_json, type(test_5))
    