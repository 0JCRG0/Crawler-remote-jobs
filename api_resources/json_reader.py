import json
import requests
import pretty_errors

#Request the api...
url = 'https://cryptojobslist.com/_next/data/SFAWiRBm4m9fe2VR1h4AR/en/tags/remote/all.json?sort=recent&page=1&tag=remote&location=all'
#url = 'https://remoteok.com/api?api=1'

#response = requests.get(url)
headers = {"User-Agent": "my-app"}
#headers = {"User-Agent"}
response = requests.get(url, headers=headers)
if response.status_code == 200:
    data = json.loads(response.text)
    #data = response.json()

    pretty_json = json.dumps(data, indent=4)
    print(pretty_json, type(data))
else:
    print("aaaa")