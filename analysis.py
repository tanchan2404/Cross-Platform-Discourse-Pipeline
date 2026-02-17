import os
import psycopg2
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from datetime import datetime, timedelta

class ToxicityAnalyzer:
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables!")
    
    def get_connection(self):
        #create a database connection
        return psycopg2.connect(dsn=self.database_url)
    
    def get_toxicity_distribution(self, platform='all', start_date=None, end_date=None):
        #Analysis 1: Get toxicity score distributions
        conn = self.get_connection()

        # need to query different tables depending on platform
        queries = []
        
        if platform in ['4chan', 'all']:
            # Query 4chan posts table
            query_4chan = """
                SELECT 
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    '4chan' as platform
                FROM posts
                WHERE toxicity_scores IS NOT NULL
                AND (toxicity_scores->>'toxicity')::float IS NOT NULL
            """
            # Add date filters if provided (use DATE() to compare only date part)
            if start_date:
                query_4chan += f" AND DATE(created_at) >= '{start_date}'"
            if end_date:
                query_4chan += f" AND DATE(created_at) <= '{end_date}'"
            queries.append(query_4chan)
        
        if platform in ['reddit', 'all']:
            # Query Reddit posts and comments
            query_reddit = """
                SELECT 
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    'reddit' as platform
                FROM reddit_posts
                WHERE toxicity_scores IS NOT NULL
                AND (toxicity_scores->>'toxicity')::float IS NOT NULL
            """
            if start_date:
                query_reddit += f" AND DATE(created_at) >= '{start_date}'"
            if end_date:
                query_reddit += f" AND DATE(created_at) <= '{end_date}'"
            queries.append(query_reddit)
            
            #include Reddit comments
            query_comments = """
                SELECT 
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    'reddit' as platform
                FROM reddit_comments
                WHERE toxicity_scores IS NOT NULL
                AND (toxicity_scores->>'toxicity')::float IS NOT NULL
            """
            if start_date:
                query_comments += f" AND DATE(created_at) >= '{start_date}'"
            if end_date:
                query_comments += f" AND DATE(created_at) <= '{end_date}'"
            queries.append(query_comments)
        
        full_query = " UNION ALL ".join(queries)
        
        df = pd.read_sql(full_query, conn)
        conn.close()
        
        if df.empty:
            return {'error': 'No data found for the selected filters'}
        
        # Prepare data for Plotly
        result = {
            'histogram': {},
            'cdf': {}
        }
        
        # Calculate histogram for each platform
        for plat in df['platform'].unique():
            plat_data = df[df['platform'] == plat]['toxicity']
            
            bins = np.arange(0, 1.05, 0.05)
            counts, edges = np.histogram(plat_data, bins=bins)
            
            result['histogram'][plat] = {
                'x': edges[:-1].tolist(),  
                'y': counts.tolist(),      
                'name': plat
            }
            
            # Calculate CDF 
            sorted_scores = np.sort(plat_data)
            cumulative = np.arange(1, len(sorted_scores) + 1) / len(sorted_scores)
            
            result['cdf'][plat] = {
                'x': sorted_scores.tolist(),
                'y': cumulative.tolist(),
                'name': plat
            }
        
        #add summary statistics
        result['stats'] = {
            'total_posts': len(df),
            'platforms': df['platform'].value_counts().to_dict(),
            'mean_toxicity': {
                plat: df[df['platform'] == plat]['toxicity'].mean()
                for plat in df['platform'].unique()
            }
        }
        
        return result
    
    def get_keyword_frequency(self, platform='all', threshold=0.35, keywords=None):
        #Analysis 2: Compare keyword usage in high vs low toxicity posts
        #Do certain words appear more often in toxic discussions?

        # Default keywords if none provided
        if not keywords or len(keywords) == 0:
            keywords = ['ukraine', 'russia', 'gaza', 'israel', 'china', 
                       'trump', 'election', 'jew', 'muslim', 'immigrant']
        
        conn = self.get_connection()
        
        # query to get posts with their text and toxicity
        queries = []
        
        if platform in ['4chan', 'all']:
            queries.append("""
                SELECT 
                    data->>'com' as text,
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    '4chan' as platform
                FROM posts
                WHERE toxicity_scores IS NOT NULL
                AND data->>'com' IS NOT NULL
            """)
        
        if platform in ['reddit', 'all']:
            queries.append("""
                SELECT 
                    title || ' ' || COALESCE(data->>'selftext', '') as text,
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    'reddit' as platform
                FROM reddit_posts
                WHERE toxicity_scores IS NOT NULL
            """)
        
        full_query = " UNION ALL ".join(queries)
        df = pd.read_sql(full_query, conn)
        conn.close()
        
        if df.empty:
            return {'error': 'No data found'}
        
        #split into high and low toxicity groups
        high_toxic = df[df['toxicity'] > threshold]
        low_toxic = df[df['toxicity'] <= threshold]
        
        # Count keyword occurrences
        result = {
            'keywords': keywords,
            'high_toxicity': {},
            'low_toxicity': {},
            'ratio': {} 
        }
        
        for keyword in keywords:
            high_count = high_toxic['text'].str.contains(keyword, case=False, na=False).sum()
            low_count = low_toxic['text'].str.contains(keyword, case=False, na=False).sum()
            
            # Normalize by group size 
            high_freq = (high_count / len(high_toxic) * 100) if len(high_toxic) > 0 else 0
            low_freq = (low_count / len(low_toxic) * 100) if len(low_toxic) > 0 else 0
            
            result['high_toxicity'][keyword] = high_freq
            result['low_toxicity'][keyword] = low_freq
            result['ratio'][keyword] = (high_freq / low_freq) if low_freq > 0 else 0
        
        result['stats'] = {
            'threshold': threshold,
            'high_toxic_count': len(high_toxic),
            'low_toxic_count': len(low_toxic)
        }
        
        return result
    
    def get_multi_attribute_toxicity(self, platform='all', start_date=None, 
                                     end_date=None, show_ratio=False):
        """
        Analysis 3: Break down toxicity into 6 different types
        
        Perspective API scores
        -toxicity - overall rudeness
        -severe_toxicity - very hateful content
        -identity_attack - targeting groups
        -insult - personal attacks
        -profanity - swear words
        -threat - violence/intimidation
        
        FIXED: Now properly includes Reddit comments alongside posts
        """
        conn = self.get_connection()
        
        # Attributes to extract from toxicity_scores JSON
        attributes = ['toxicity', 'severe_toxicity', 'identity_attack', 
                     'insult', 'profanity', 'threat']
        
        queries = []
        
        if platform in ['4chan', 'all']:
            select_parts = [f"(toxicity_scores->>'{attr}')::float as {attr}" 
                           for attr in attributes]
            query_4chan = f"""
                SELECT 
                    {', '.join(select_parts)},
                    '4chan' as platform
                FROM posts
                WHERE toxicity_scores IS NOT NULL
            """
            if start_date:
                query_4chan += f" AND DATE(created_at) >= '{start_date}'"
            if end_date:
                query_4chan += f" AND DATE(created_at) <= '{end_date}'"
            queries.append(query_4chan)
        
        if platform in ['reddit', 'all']:
            select_parts = [f"(toxicity_scores->>'{attr}')::float as {attr}" 
                           for attr in attributes]
            
            # Reddit posts
            query_reddit_posts = f"""
                SELECT 
                    {', '.join(select_parts)},
                    'reddit' as platform
                FROM reddit_posts
                WHERE toxicity_scores IS NOT NULL
            """
            if start_date:
                query_reddit_posts += f" AND created_at >= '{start_date}'"
            if end_date:
                query_reddit_posts += f" AND created_at <= '{end_date}'"
            queries.append(query_reddit_posts)
            
            # Reddit comments - THIS WAS MISSING!
            query_reddit_comments = f"""
                SELECT 
                    {', '.join(select_parts)},
                    'reddit' as platform
                FROM reddit_comments
                WHERE toxicity_scores IS NOT NULL
            """
            if start_date:
                query_reddit_comments += f" AND DATE(created_at) >= '{start_date}'"
            if end_date:
                query_reddit_comments += f" AND DATE(created_at) <= '{end_date}'"
            queries.append(query_reddit_comments)
        
        full_query = " UNION ALL ".join(queries)
        df = pd.read_sql(full_query, conn)
        conn.close()
        
        if df.empty:
            return {'error': 'No data found'}
        
        #calculate means for each attribute by platform
        result = {'attributes': attributes}
        
        for plat in df['platform'].unique():
            plat_data = df[df['platform'] == plat]
            result[plat] = {
                attr: plat_data[attr].mean() 
                for attr in attributes
            }
        
        # Calculate ratios 
        if show_ratio and '4chan' in result and 'reddit' in result:
            result['ratio'] = {
                attr: result['4chan'][attr] / result['reddit'][attr] 
                      if result['reddit'][attr] > 0 else 0
                for attr in attributes
            }
        
        return result
    
    def get_temporal_analysis(self, keyword='ukraine', window_days=3, 
                             platform='all', metric='volume'):
        #Analysis 4: Show how toxicity changes with time
        #finds the peak day for a keyword and shows activity ±3 days around it
        
        conn = self.get_connection()
        
        # find posts containing the keyword
        queries = []
        
        if platform in ['4chan', 'all']:
            queries.append(f"""
                SELECT 
                    DATE(created_at) as date,
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    '4chan' as platform
                FROM posts
                WHERE data->>'com' ILIKE '%{keyword}%'
                AND toxicity_scores IS NOT NULL
            """)
        
        if platform in ['reddit', 'all']:
            queries.append(f"""
                SELECT 
                    DATE(created_at) as date,
                    (toxicity_scores->>'toxicity')::float as toxicity,
                    'reddit' as platform
                FROM reddit_posts
                WHERE (title ILIKE '%{keyword}%' OR data->>'selftext' ILIKE '%{keyword}%')
                AND toxicity_scores IS NOT NULL
            """)
        
        full_query = " UNION ALL ".join(queries)
        df = pd.read_sql(full_query, conn)
        conn.close()
        
        if df.empty:
            return {'error': f'No posts found containing keyword: {keyword}'}
        
        # find peak day for each platform
        result = {
            'keyword': keyword,
            'window_days': window_days,
            'platforms': {}
        }
        
        for plat in df['platform'].unique():
            plat_data = df[df['platform'] == plat]
            
            # Count posts per day
            daily_counts = plat_data.groupby('date').size()
            peak_date = daily_counts.idxmax()
            
            # Get window around peak
            start_date = peak_date - timedelta(days=window_days)
            end_date = peak_date + timedelta(days=window_days)
            
            window_data = plat_data[
                (plat_data['date'] >= start_date) & 
                (plat_data['date'] <= end_date)
            ]
            
            if metric == 'volume':
                daily_metric = window_data.groupby('date').size()
            else:
                daily_metric = window_data.groupby('date')['toxicity'].mean()

            dates = daily_metric.index.tolist()
            days_from_peak = [(d - peak_date).days for d in dates]
            values = daily_metric.values.tolist()
            
            result['platforms'][plat] = {
                'peak_date': str(peak_date),
                'days_from_peak': days_from_peak,
                'values': values,
                'metric': metric
            }
        
        return result
    
    def get_tfidf_toxic_words(self, platform='4chan', threshold=0.35, top_n=20):
        """
        TF-IDF Analysis to find words that define toxic speech
        
        TF-IDF finds words that are:
        - Common in toxic posts
        - Rare in non-toxic posts
        """
        conn = self.get_connection()
        
        # Get posts with text and toxicity
        if platform == '4chan':
            query = """
                SELECT 
                    data->>'com' as text,
                    (toxicity_scores->>'toxicity')::float as toxicity
                FROM posts
                WHERE toxicity_scores IS NOT NULL
                AND data->>'com' IS NOT NULL
                AND LENGTH(data->>'com') > 10
                LIMIT 10000
            """
        else:  # reddit
            query = """
                (SELECT 
                    title || ' ' || COALESCE(data->>'selftext', '') as text,
                    (toxicity_scores->>'toxicity')::float as toxicity
                FROM reddit_posts
                WHERE toxicity_scores IS NOT NULL
                AND title IS NOT NULL
                AND LENGTH(title || ' ' || COALESCE(data->>'selftext', '')) > 10
                LIMIT 7000)
                UNION ALL
                (SELECT 
                    data->>'body' as text,
                    (toxicity_scores->>'toxicity')::float as toxicity
                FROM reddit_comments
                WHERE toxicity_scores IS NOT NULL
                AND data->>'body' IS NOT NULL
                AND LENGTH(data->>'body') > 10
                LIMIT 3000)
            """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty or len(df) < 50:
            return {'error': 'Insufficient data for TF-IDF analysis'}
        
        # Split into toxic and non-toxic
        toxic_posts = df[df['toxicity'] > threshold]['text'].tolist()
        nontoxic_posts = df[df['toxicity'] <= threshold]['text'].tolist()
        
        if len(toxic_posts) < 10 or len(nontoxic_posts) < 10:
            return {'error': 'Not enough posts in toxic or non-toxic group'}
        
        # Custom stop words to filter HTML markup and common non-toxic terms
        custom_stop_words = list(ENGLISH_STOP_WORDS) + [
            'span', 'br', 'quote', 'gt', 'class', 'href', 
            '039', 'quot', 'div', 'greentext', 'http', 'https',
            'www', 'com', 'html', 'link', 'post', 'thread'
        ]

        # Calculate TF-IDF for toxic posts
        vectorizer = TfidfVectorizer(
            max_features=500,     
            stop_words=custom_stop_words,
            min_df=2,              
            max_df=0.8,            
            ngram_range=(1, 2),     
            token_pattern=r'\b[a-zA-Z]{2,}\b'
        )
        
        toxic_tfidf = vectorizer.fit_transform(toxic_posts)
        
        feature_names = vectorizer.get_feature_names_out()
        
        # Calculate mean TF-IDF score for each word across all toxic posts
        mean_tfidf = toxic_tfidf.mean(axis=0).A1
        
        # Get top N words
        top_indices = mean_tfidf.argsort()[-top_n:][::-1]
        top_words = [feature_names[i] for i in top_indices]
        top_scores = [mean_tfidf[i] for i in top_indices]
        
        return {
            'platform': platform,
            'threshold': threshold,
            'toxic_posts_analyzed': len(toxic_posts),
            'nontoxic_posts_analyzed': len(nontoxic_posts),
            'top_words': top_words,
            'scores': top_scores,
            'interpretation': (
                'These words appear frequently in toxic posts but rarely in normal posts. '
                'Higher scores indicate stronger association with toxicity.'
            )
        }
        