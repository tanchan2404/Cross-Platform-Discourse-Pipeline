import os, datetime, logging, psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

from pyfaktory import Client, Consumer, Producer, Job
from reddit_client import RedditJSON

from dotenv import load_dotenv

register_adapter(dict, Json)
load_dotenv()

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), 20))
log = logging.getLogger("reddit-json-worker")

DATABASE_URL = os.getenv("DATABASE_URL")
FACTORY_SERVER_URL = os.getenv("FACTORY_SERVER_URL")

def crawl_subreddit_listing(sub, after=None):
    rc = RedditJSON()
    payload = rc.list_new(sub, after=after, limit=100)
    if not payload or "data" not in payload:
        log.warning(f"no listing data for r/{sub} after={after}")
        return

    data = payload["data"]
    next_after = data.get("after")
    posts = data.get("children", [])

    with Client(faktory_url=FACTORY_SERVER_URL, role="producer") as c:
        p = Producer(client=c)
        for ch in posts:
            post = ch.get("data", {})
            pid = post.get("id")
            if not pid: 
                continue
            p.push(Job(jobtype="crawl_submission_json", args=(sub, pid), queue="reddit-json"))

        if next_after:
            p.push(Job(jobtype="crawl_subreddit_listing", args=(sub, next_after), queue="reddit-json"))
        
        # reschedule listing in 5 minutes
        run_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=5)).isoformat()[:-7] + "Z"
        p.push(Job(jobtype="crawl_subreddit_listing",
                  args=(sub,),              # start from the top again (after=None)
                  queue="reddit-json",
                  at=run_at))
    

def crawl_submission_json(sub, post_id):
    rc = RedditJSON()
    thread = rc.comments(post_id, sort="new", depth=1, limit=500)
    if not thread or not isinstance(thread, list) or not thread[0]["data"]["children"]:
        log.info(f"no submission body for r/{sub} {post_id}")
        return

    submission = thread[0]["data"]["children"][0]["data"]

    conn = psycopg2.connect(dsn=DATABASE_URL); cur = conn.cursor()
    q = """
    INSERT INTO reddit_posts (subreddit, post_id, created_at, author, title, data) VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (created_at, subreddit, post_id) DO NOTHING
    """
    created = datetime.datetime.utcfromtimestamp(submission["created_utc"]).replace(tzinfo=datetime.timezone.utc)
    cur.execute(q, (sub, post_id, created, submission.get("author"), submission.get("title"), Json(submission)))
    conn.commit(); 
    cur.close(); 
    conn.close()

    # enqueue comments crawl
    with Client(faktory_url=FACTORY_SERVER_URL, role="producer") as c:
        Producer(client=c).push(Job(jobtype="crawl_comments_json", args=(sub, post_id), queue="reddit-json"))

def crawl_comments_json(sub, post_id):
    rc = RedditJSON()
    thread = rc.comments(post_id, sort="new", depth=1, limit=500)
    if not thread or len(thread) < 2:
        return
    comments = thread[1]["data"]["children"]

    conn = psycopg2.connect(dsn=DATABASE_URL); cur = conn.cursor()
    q = """
    INSERT INTO reddit_comments (subreddit, post_id, comment_id, created_at, data) VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (created_at, post_id, comment_id) DO NOTHING
    """
    rows = []
    for c in comments:
        if c.get("kind") != "t1": 
            continue
        d = c["data"]
        cid = d["id"]
        created = datetime.datetime.utcfromtimestamp(d["created_utc"]).replace(tzinfo=datetime.timezone.utc)
        rows.append((sub, post_id, cid, created, Json(d)))
    if rows:
        cur.executemany(q, rows); 
        conn.commit()
    cur.close(); conn.close()

if __name__ == "__main__":
    with Client(faktory_url=FACTORY_SERVER_URL, role="consumer") as cl:
        consumer = Consumer(client=cl, queues=["reddit-json"], concurrency=3)
        consumer.register("crawl_subreddit_listing", crawl_subreddit_listing)
        consumer.register("crawl_submission_json", crawl_submission_json)
        consumer.register("crawl_comments_json", crawl_comments_json)
        consumer.run()
