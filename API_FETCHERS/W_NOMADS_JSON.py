import requests
import json
import pretty_errors
import pandas as pd

def W_NOMADS_API():
    api = "https://www.workingnomads.com/api/exposed_jobs/"
    print(f"Fetching...{api}")
    response = requests.get(api)
    if response.status_code == 200:
        data = json.loads(response.text)
        #pretty_json = json.dumps(data, indent=4)
        #print(pretty_json, type(data))
        
        #Loop through the list of dict            
        all_jobs = []
        titles = []
        links = []
        pubdates = []
        locations = []
        descriptions = []
        for job in data:
            titles.append(job["title"])
            links.append(job["url"])
            pubdates.append(job["pub_date"])
            locations.append(job["location"])
            descriptions.append(job["tags"])
            #Put it all together...
            all_jobs = {'title': titles, 'link':links, 'pubdate': pubdates, 'location': locations, 'description': descriptions}
        return all_jobs
    else:
        print("Error connecting to API:", response.status_code)

#to df
df = pd.DataFrame(W_NOMADS_API())
    
directory = "./OUTPUTS/"
df.to_csv(f"{directory}W_NOMADS.csv", index=False)