#how to run- python3 generate_tables.py
#summary stats table for project 2 report

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def generate_summary_statistics_table():
    #generate summary statistics table
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
  
    #------4chan stats-----
    print("Collecting 4chan stats...")
    
    #total posts
    cur.execute("SELECT COUNT(*) FROM posts")
    chan_total = cur.fetchone()[0]
    
    #date range
    cur.execute("SELECT MIN(created_at), MAX(created_at) FROM posts")
    chan_start, chan_end = cur.fetchone()
    
    #posts with toxicity scores
    cur.execute("SELECT COUNT(*) FROM posts WHERE toxicity_scores IS NOT NULL")
    chan_toxicity_count = cur.fetchone()[0]
    
    #average toxicity
    cur.execute("""
        SELECT AVG((toxicity_scores->>'toxicity')::float)
        FROM posts WHERE toxicity_scores IS NOT NULL
    """)
    chan_avg_tox = cur.fetchone()[0] or 0
    
    #----reddit posts stats------
    print("Collecting Reddit posts stats...")
    
    #total posts
    cur.execute("SELECT COUNT(*) FROM reddit_posts")
    reddit_posts_total = cur.fetchone()[0]
    
    #date range
    cur.execute("SELECT MIN(created_at), MAX(created_at) FROM reddit_posts")
    reddit_start, reddit_end = cur.fetchone()
    
    #posts with toxicity scores
    cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE toxicity_scores IS NOT NULL")
    reddit_toxicity_count = cur.fetchone()[0]
    
    #average toxicity
    cur.execute("""
        SELECT AVG((toxicity_scores->>'toxicity')::float)
        FROM reddit_posts WHERE toxicity_scores IS NOT NULL
    """)
    reddit_avg_tox = cur.fetchone()[0] or 0
    
    #----reddit comments stats-----
    print("Collecting Reddit comments stats...")
    
    #total comments
    cur.execute("SELECT COUNT(*) FROM reddit_comments")
    reddit_comments_total = cur.fetchone()[0]
    
    #comments with toxicity scores
    cur.execute("SELECT COUNT(*) FROM reddit_comments WHERE toxicity_scores IS NOT NULL")
    reddit_comments_toxicity_count = cur.fetchone()[0]
    
    #average toxicity
    cur.execute("""
        SELECT AVG((toxicity_scores->>'toxicity')::float)
        FROM reddit_comments WHERE toxicity_scores IS NOT NULL
    """)
    reddit_comments_avg_tox = cur.fetchone()[0] or 0
    
    cur.close()
    conn.close()
    
    #creating table
    stats = {
        'Metric': [
            'Total Posts/Comments',
            'Collection Start',
            'Collection End',
            'Duration (days)',
            'Posts with Toxicity',
            'Toxicity Coverage (%)',
            'Avg Toxicity Score'
        ],
        '4chan': [
            f'{chan_total:,}',
            chan_start.strftime('%Y-%m-%d') if chan_start else 'N/A',
            chan_end.strftime('%Y-%m-%d') if chan_end else 'N/A',
            (chan_end - chan_start).days if chan_start and chan_end else 'N/A',
            f'{chan_toxicity_count:,}',
            f'{(chan_toxicity_count/chan_total*100):.1f}' if chan_total > 0 else '0',
            f'{chan_avg_tox:.3f}'
        ],
        'Reddit Posts': [
            f'{reddit_posts_total:,}',
            reddit_start.strftime('%Y-%m-%d') if reddit_start else 'N/A',
            reddit_end.strftime('%Y-%m-%d') if reddit_end else 'N/A',
            (reddit_end - reddit_start).days if reddit_start and reddit_end else 'N/A',
            f'{reddit_toxicity_count:,}',
            f'{(reddit_toxicity_count/reddit_posts_total*100):.1f}' if reddit_posts_total > 0 else '0',
            f'{reddit_avg_tox:.3f}'
        ],
        'Reddit Comments': [
            f'{reddit_comments_total:,}',
            'N/A',
            'N/A',
            'N/A',
            f'{reddit_comments_toxicity_count:,}',
            f'{(reddit_comments_toxicity_count/reddit_comments_total*100):.1f}' if reddit_comments_total > 0 else '0',
            f'{reddit_comments_avg_tox:.3f}'
        ]
    }
    
    df = pd.DataFrame(stats)
    
    print("\n" + "-"*25)
    print(df.to_string(index=False))
    print("-"*25 + "\n")

def generate_daily_post_counts_table():
    #generate daily post counts for Nov 1-14
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
    
    print("-"*50)
    print("DAILY POST COUNTS (November 1-14, 2025)")
    print("-"*50 + "\n")
    
    #4chan daily counts
    cur.execute("""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as post_count,
            COUNT(*) FILTER (WHERE data->>'resto' = '0') as new_threads
        FROM posts
        WHERE board_name = 'pol'
        AND created_at >= '2025-11-01' AND created_at < '2025-11-15'
        GROUP BY DATE(created_at)
        ORDER BY date
    """)
    chan_daily = cur.fetchall()
    
    cur.close()
    conn.close()
    
    #create dataframe
    data = {
        'Date': [],
        'Total Posts': [],
        'New Threads Created': []
    }
    
    for row in chan_daily:
        data['Date'].append(row[0].strftime('%Y-%m-%d'))
        data['Total Posts'].append(f"{row[1]:,}")
        data['New Threads Created'].append(f"{row[2]:,}")
    
    df = pd.DataFrame(data)
    
    print(df.to_string(index=False))
    print("\n" + "-"*50 + "\n")

def generate_keyword_table():
    #generate keyword frequency table
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
    
    #keywords to track
    keywords = ['ukraine', 'russia', 'gaza', 'israel', 'trump', 
                'election', 'china', 'taiwan']
    
    print("-"*25)
    print("KEYWORD FREQUENCIES")
    print("-"*25 + "\n")
    
    #data dictionary
    data = {'Keyword': [], '4chan': [], 'Reddit Posts': [], 'Reddit Comments': [], 'Total': []}
    
    for keyword in keywords:
        data['Keyword'].append(keyword.capitalize())
        
        #count in 4chan
        cur.execute(f"SELECT COUNT(*) FROM posts WHERE data::text ILIKE '%{keyword}%'")
        chan = cur.fetchone()[0]
        data['4chan'].append(f'{chan:,}')
        
        #count in Reddit posts
        cur.execute(f"SELECT COUNT(*) FROM reddit_posts WHERE data::text ILIKE '%{keyword}%'")
        reddit = cur.fetchone()[0]
        data['Reddit Posts'].append(f'{reddit:,}')
        
        #count in Reddit comments
        cur.execute(f"SELECT COUNT(*) FROM reddit_comments WHERE data::text ILIKE '%{keyword}%'")
        comments = cur.fetchone()[0]
        data['Reddit Comments'].append(f'{comments:,}')
        
        #total across all platforms
        data['Total'].append(f'{chan + reddit + comments:,}')
    
    cur.close()
    conn.close()
    
    df = pd.DataFrame(data)
    
    print(df.to_string(index=False))
    print("-"*25 + "\n")
    

if __name__ == "__main__":
    generate_summary_statistics_table()
    generate_daily_post_counts_table()
    generate_keyword_table()