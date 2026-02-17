import os, time, logging, requests, random

log = logging.getLogger("reddit-json")
log.setLevel(getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), 20))
log.addHandler(logging.StreamHandler())

BASE = "https://www.reddit.com"

#rate limit- 1 req/sec average-- 429s
MIN_SLEEP = 1.0

class RedditJSON:
    def __init__(self):
        self.s = requests.Session()

    def _get(self, url, **kwargs):
        for attempt in range(6):
            try:
                r = self.s.get(url, timeout=20, **kwargs)
                # 429/403: backoff and retry
                if r.status_code in (429, 403):
                    wait = min(60, (2 ** attempt)) + random.uniform(0, 0.5)
                    log.warning(f"{r.status_code} on {url}; sleeping {wait:.1f}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                time.sleep(MIN_SLEEP + random.uniform(0, 0.4))
                return r.json()
            except requests.RequestException as e:
                wait = min(30, (2 ** attempt)) + random.uniform(0, 0.5)
                log.warning(f"HTTP error {e}; retry in {wait:.1f}s")
                time.sleep(wait)
        log.error(f"giving up on {url}")
        return None

    def list_new(self, sub, after=None, limit=100):
        params = {"limit": limit, "raw_json": 1}
        if after: params["after"] = after
        url = f"{BASE}/r/{sub}/new.json"
        return self._get(url, params=params)

    def comments(self, post_id, sort="new", depth=1, limit=500):
        params = {"sort": sort, "depth": depth, "limit": limit, "raw_json": 1}
        url = f"{BASE}/comments/{post_id}.json"
        return self._get(url, params=params)

if __name__ == "__main__":
    client = RedditJSON()

    # get latest posts from a subreddit
    listing = client.list_new("politics", limit=5)
    #print(listing)

    print(client.comments("17n4i2t"))