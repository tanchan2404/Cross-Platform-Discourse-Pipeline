import os, sys, logging
from pyfaktory import Client, Consumer, Job, Producer

# import time
# import random

logger = logging.getLogger("faktory test")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


if __name__ == "__main__":
    boards = sys.argv[1:] or [b.strip() for b in os.getenv("CHAN_BOARDS", "pol").split(",") if b.strip()]
    faktory_server_url = os.getenv("FACTORY_SERVER_URL", "tcp://:password@localhost:7419")
    print(f"Cold starting catalog crawl for board {boards}")
    with Client(faktory_url=faktory_server_url, role="producer") as c:
        p = Producer(client=c)
        for board in boards:
            logger.info(f"Seeding crawl_thread_listing for /{board}/")
            p.push(Job(jobtype="crawl_thread_listing", args=(board,), queue="crawl-thread-listing"))

    
