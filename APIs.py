import requests
import json
import pretty_errors
import pandas as pd
import psycopg2
import timeit
from utils.handy import to_postgre

def w_nomads():
    print("\n", "W_NOMADS starting now.")
    #Start the timer
    start_time = timeit.default_timer()

    def api_fetcher():

        api = "https://www.workingnomads.com/api/exposed_jobs/"
        #print("\n", f"Fetching...{api}", "\n")
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

    data = api_fetcher()

    #to df
    df = pd.DataFrame(data)

    directory = "./OUTPUTS/"
    df.to_csv(f"{directory}W_NOMADS.csv", index=False)

    ## PostgreSQL
    to_postgre(df)

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"W_NOMADS is done! all in: {elapsed_time:.2f} seconds", "\n")

if __name__ == "__main__":
    w_nomads()