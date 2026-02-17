CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS reddit_posts (
  subreddit TEXT NOT NULL,
  post_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  data JSONB NOT NULL
);

SELECT create_hypertable('reddit_posts','created_at',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS reddit_posts_uniq
  ON reddit_posts (created_at, subreddit, post_id);

CREATE TABLE IF NOT EXISTS reddit_comments (
  subreddit TEXT NOT NULL,
  post_id TEXT NOT NULL,
  comment_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  data JSONB NOT NULL
);

SELECT create_hypertable('reddit_comments','created_at',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS reddit_comments_uniq
  ON reddit_comments (created_at, post_id, comment_id);