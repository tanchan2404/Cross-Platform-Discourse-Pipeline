import os, sys
from pyfaktory import Client, Producer, Job

subs = sys.argv[1:] or [s.strip() for s in os.getenv("REDDIT_SUBS","politics,worldnews,geopolitics").split(",") if s.strip()]
url = os.getenv("FACTORY_SERVER_URL","tcp://:password@localhost:7419")

with Client(faktory_url=url, role="producer") as c:
    p = Producer(client=c)
    for sub in subs:
        p.push(Job(jobtype="crawl_subreddit_listing", args=(sub,), queue="reddit-json"))
        print(f"seeded r/{sub}")
