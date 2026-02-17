import os
import psycopg2
import csv
from dotenv import load_dotenv

load_dotenv()

#keywords based on whats happening rn
KEYWORDS = ['ukraine', 'russia', 'putin', 'zelensky', 'gaza', 'israel', 'hamas', 'palestine', 'taiwan', 'china', 'election', 'trump', 'harris', 'biden', 'syria', 'iran', 'nuclear', 'war', 'conflict', 'invasion', 'military']

def get_4chan_posts(conn, keywords):
    cur = conn.cursor()
    
    #build the where clause with OR for each keyword
    conditions = []
    for k in keywords:
        conditions.append(f"data::text ILIKE '%{k}%'")
    where_clause = " OR ".join(conditions)
    
    query = f"""
    SELECT board_name, thread_number, post_number, created_at, data->>'com' as comment_text
    FROM posts
    WHERE {where_clause}
    ORDER BY created_at
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    # save to csv
    with open('filtered_4chan.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['board', 'thread', 'post', 'timestamp', 'text', 'platform'])
        
        for row in rows:
            board, thread, post_num, timestamp, text = row
            # clean up the text a bit
            if text:
                text = text.replace('\n', ' ')[:500]
            writer.writerow([board, thread, post_num, timestamp, text, '4chan'])
    
    print(f"\nsaved {len(rows)} 4chan posts")
    cur.close()
    return len(rows)

def get_reddit_posts(conn, keywords):
    cur = conn.cursor()
    
    conditions = []
    for k in keywords:
        conditions.append(f"data::text ILIKE '%{k}%'")
    where_clause = " OR ".join(conditions)
    
    query = f"""
    SELECT subreddit, post_id, created_at, title, data->>'selftext' as body
    FROM reddit_posts
    WHERE {where_clause}
    ORDER BY created_at
    """

    cur.execute(query)
    rows = cur.fetchall()
    
    with open('filtered_reddit.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['subreddit', 'post_id', 'timestamp', 'title', 'body', 'platform'])
        
        for row in rows:
            subreddit, post_id, timestamp, title, body = row
            if title:
                title = title.replace('\n', ' ')[:200]
            if body:
                body = body.replace('\n', ' ')[:500]
            writer.writerow([subreddit, post_id, timestamp, title, body, 'reddit'])
    
    print(f"saved {len(rows)} reddit posts")
    cur.close()
    return len(rows)

def get_reddit_comments(conn, keywords):
    cur = conn.cursor()
    
    conditions = []
    for k in keywords:
        conditions.append(f"data::text ILIKE '%{k}%'")
    where_clause = " OR ".join(conditions)
    
    query = f"""
    SELECT subreddit, post_id, comment_id, created_at, data->>'body' as body
    FROM reddit_comments
    WHERE {where_clause}
    ORDER BY created_at
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    with open('filtered_reddit_comments.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['subreddit', 'post_id', 'comment_id', 'timestamp', 'body', 'platform'])
        
        for row in rows:
            subreddit, post_id, comment_id, timestamp, body = row
            if body:
                body = body.replace('\n', ' ')[:500]
            writer.writerow([subreddit, post_id, comment_id, timestamp, body, 'reddit'])
    
    print(f"saved {len(rows)} reddit comments")
    cur.close()
    return len(rows)

# print stats 
def print_keyword_stats(conn):
    cur = conn.cursor()
    
    print("\nKeyword Statistics:\n")
    print(f"{'Keyword':<15} {'4chan':<10} {'Reddit Posts':<15} {'Reddit Comments':<15} {'Total'}")
    print("-" * 70)
    
    for keyword in KEYWORDS:
        # count in 4chan
        cur.execute(f"SELECT COUNT(*) FROM posts WHERE data::text ILIKE '%{keyword}%'")
        chan_count = cur.fetchone()[0]
        
        # count in reddit posts
        cur.execute(f"SELECT COUNT(*) FROM reddit_posts WHERE data::text ILIKE '%{keyword}%'")
        reddit_posts = cur.fetchone()[0]
        
        # count in reddit comments
        cur.execute(f"SELECT COUNT(*) FROM reddit_comments WHERE data::text ILIKE '%{keyword}%'")
        reddit_comments = cur.fetchone()[0]
        
        total = chan_count + reddit_posts + reddit_comments
        
        if total > 0:
            print(f"{keyword:<15} {chan_count:<10} {reddit_posts:<15} {reddit_comments:<15} {total}")
    
    cur.close()

if __name__ == "__main__":
    DATABASE_URL = os.environ.get("DATABASE_URL")
    conn = psycopg2.connect(dsn=DATABASE_URL)
    
    print_keyword_stats(conn)
    
    chan_count = get_4chan_posts(conn, KEYWORDS)
    reddit_posts_count = get_reddit_posts(conn, KEYWORDS)
    reddit_comments_count = get_reddit_comments(conn, KEYWORDS)
    
    print(f"\nTotal4chan posts: {chan_count}")
    print(f"Total Reddit posts: {reddit_posts_count}")
    print(f"Total Reddit comments: {reddit_comments_count}")
    print(f"Total items: {chan_count + reddit_posts_count + reddit_comments_count}")
    
    conn.close()