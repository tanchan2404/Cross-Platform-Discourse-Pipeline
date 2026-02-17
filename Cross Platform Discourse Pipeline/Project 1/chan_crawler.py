import datetime
from chan_client import ChanClient
import os
import time
from pyfaktory import Client, Consumer, Job, Producer
import logging

# these three lines allow psycopg to insert a dict into
# a jsonb coloumn
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

register_adapter(dict, Json)

from dotenv import load_dotenv

load_dotenv()



logger = logging.getLogger("4chan client")
logger.propagate = False

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_str, logging.INFO)

logger.setLevel(numeric_level)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


# get db url
DATABASE_URL = os.environ.get("DATABASE_URL")
FACTORY_SERVER_URL = os.environ.get("FACTORY_SERVER_URL")


def threads_list_to_thread_number(thread_list):
    thread_numbers = set()
    for page in thread_list:
        for thread in page["threads"]:
            thread_numbers.add(thread["no"])
    return thread_numbers


"""enqueue a thread crawl job to get the posts in a thread"""


def enqueue_crawl_thread(board, thread_number):
    client = ChanClient()
    # we probably want to save teh output of get_thread somewherE (e.g., database)
    logger.info(f"Getting thread /{board}/{thread_number}")
    thread = client.get_thread(board, thread_number)
    # id BIGSERIAL NOT NULL,
    # board_name TEXT NOT NULL,
    # thread_number BIGINT NOT NULL,
    # post_number BIGINT NOT NULL,
    # created_at TIMESTAMPTZ NOT NULL,
    # data JSONB NOT NULL

    # thead is a json object, that has one field called `posts`
    # which is an array of all the posts in the thread

    if not thread:
        logger.warning("Empty thread!")
        return

    import psycopg2
    from psycopg2.extras import Json
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()


    q = """
    INSERT INTO posts (board_name, thread_number, post_number, created_at, data)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    """
    rows = []
    for post in thread["posts"]:
        post_number = post["no"]
        created_at = datetime.datetime.fromtimestamp(post["time"])
        rows.append((board, thread_number, post_number, created_at, post))
    if rows:
        cur.executemany(q, rows)
        conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Inserted {len(rows)} posts for /{board}/{thread_number}")


"""enqueue a thread list carwl to get the live threads on a board"""


def enqueue_crawl_threads_listing(board, old_threads=[]):
    if old_threads is None:
        old_threads = []

    client = ChanClient()
    current = threads_list_to_thread_number(client.get_threads(board))

    #get threads that existed before but are gone now)
    dead_threads = set(old_threads) - set(current) if old_threads else set()
    
    #crawl both current and dead threads (fix error from previous collection system)
    targets = current.union(dead_threads)
    
    logger.info(f"/{board}/ targets to crawl: {len(targets)} ({len(current)} current + {len(dead_threads)} dead)")

    if targets:
        with Client(faktory_url=FACTORY_SERVER_URL, role="producer") as c:
            p = Producer(client=c)
            for t in targets:
                p.push(Job(jobtype="crawl_thread", args=(board, t), queue="crawl-thread"))

    #reschedule listing in 5 minutes
    run_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=5)).isoformat()[:-7] + "Z"
    with Client(faktory_url=FACTORY_SERVER_URL, role="producer") as c:
        Producer(client=c).push(Job(
            jobtype="crawl_thread_listing",
            args=(board, list(current)),
            queue="crawl-thread-listing",
            at=run_at,
        ))


"""Get a set of the threads that are now dead"""


def get_dead_threads(board, old_threads=set()):
    if old_threads is None:
        old_threads = set()
    client = ChanClient()
    current = threads_list_to_thread_number(client.get_threads(board))
    dead = set(old_threads) - current
    if dead:
        with Client(faktory_url=FACTORY_SERVER_URL, role="producer") as c:
            p = Producer(client=c)
            for t in dead:
                p.push(Job(jobtype="crawl_thread", args=(board, t), queue="crawl-thread"))
    return dead


if __name__ == "__main__":
    # client = ChanClient()
    # old_threads = threads_list_to_thread_number(client.get_threads("pol"))
    # faktory_server_url = "tcp://:password@localhost:7419"
    boards = [b.strip() for b in os.getenv("CHAN_BOARDS", "pol").split(",") if b.strip()]
    logger.info(f"Worker starting. Boards: {boards}")

    with Client(faktory_url=FACTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(
            client=client,
            queues=["default", "crawl-thread", "crawl-thread-listing"],
            concurrency=3,
        )
        consumer.register("crawl_thread", enqueue_crawl_thread)
        consumer.register("crawl_thread_listing", enqueue_crawl_threads_listing)
        consumer.run()

    # print(f"we found dead threads: {dead_threads}")
    # loop until we've discovered some new thread

    # while True:
    #     print("starting iteration")

    #     dead_threads = get_dead_threads("pol", old_threads)
    #     faktory_server_url = "tcp://:password@localhost:7419"
    #     if len(dead_threads) > 0:
    #         print("found a dead thread")
    #         # we found a dead thread
    #         for thread in dead_threads:
    #             with Client(faktory_url=faktory_server_url, role="producer") as client:
    #                 producer = Producer(client=client)
    #                 job = Job(jobtype="crawl_thread", args=("pol", thread), queue="crawl-thread")
    #                 producer.push(job)

    #     time.sleep(10)
