import requests
import json
import pretty_errors
import pandas as pd
import psycopg2
import timeit
from utils.handy import test1_postgre, send_postgre, to_postgre

def W_NOMADS():
    #Start the timer
    start_time = timeit.default_timer()

    def API_FETCHER():

        api = "https://www.workingnomads.com/api/exposed_jobs/"
        print("\n", f"Fetching...{api}", "\n")
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

    data = API_FETCHER()

    #to df
    df = pd.DataFrame(data)
    print("\n", f"Successfully fetched {len(df)} jobs", "\n")

    print("\n", "Saving jobs into local machine as a CSV...", "\n")

    directory = "./OUTPUTS/"
    df.to_csv(f"{directory}W_NOMADS.csv", index=False)

    print("\n", f"Fetching {len(df)} cleaned jobs to PostgreSQL...", "\n")

    ## PostgreSQL

    to_postgre(df)

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"W_NOMADS is done! all in: {elapsed_time:.2f} seconds", "\n")
W_NOMADS()