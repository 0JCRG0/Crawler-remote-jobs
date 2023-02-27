# Crawler-remote-jobs
Crawlers to get remote jobs

So, this project is not finished. It is being constantly updated. But I think that it might be useful from now on.

1. There are two types of crawlers in this repo. Bs4 & selenium. 
  1.1. Bs4 is used for websites with RSS FEED available.
  1.2. Selenium is used for websites without RSS FEED (duh).
 
2. The BS4 Crawler gets the titles, links, description & pubDate of every remote job published in the 34 websites that it scraps.
3. The output is a pandas df which we use in the jupyter Notebook for some data engineering. 
4. Finally, you should have a pretty df for all the remote jobs in those websites. 

The selenium crawlers work very well but they are considerably slower. Also, they do now scrap that many websites at all. 
