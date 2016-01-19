DROP KEYSPACE bde_twitter;

CREATE KEYSPACE bde_twitter WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};
-- CREATE KEYSPACE bde_twitter WITH replication = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}; // prod

CREATE TABLE bde_twitter.twitter_source (
  account_name text,
  active boolean,
  primary key (account_name)
);

create index source_active_idx on bde_twitter.twitter_source(active);

CREATE TABLE bde_twitter.twitter_user (
  user_id bigint,
  followers_count bigint,
  friends_count bigint,
  listed_count bigint,
  name text,
  screen_name text,
  location text,
  statuses_count bigint,
  timezone text,
  primary key ((user_id))
);

create index user_screen_name_idx on bde_twitter.twitter_user(screen_name);

CREATE TABLE bde_twitter.twitter_post (
  post_id bigint,
  created_at timestamp,
  coordinates text,
  place text,
  retweet_count bigint,
  followers_when_published bigint,
  tweet text,
  language text,
  url text,
  external_links set<text>,
  account_name text,
  primary key ((post_id), language) 
);

CREATE TABLE bde_twitter.twitter_created_at_per_post (
    year_month_bucket varchar,
    created_at timestamp,
    post_id bigint,
    tweet text,
    language text,
    url text,
    account_name text,
    primary key ((year_month_bucket), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id DESC);

create index language_created_at_idx on bde_twitter.twitter_created_at_per_post(language);

CREATE TABLE bde_twitter.twitter_hashtag_per_post (
    hashtag text,
    post_id bigint,
    created_at timestamp,
    tweet text,    
    language text,
    url text,
    account_name text,
    primary key ((hashtag), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id DESC);

CREATE TABLE bde_twitter.twitter_external_urls_per_post (
    external_url text,
    post_id bigint,
    created_at timestamp,
    tweet text,
    language text,
    url text,
    account_name text,
    primary key ((external_url), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id DESC);

CREATE TABLE bde_twitter.twitter_log (
  engine_type text,
  engine_id bigint,
  started timestamp, 
  ended timestamp,
  primary key ((engine_type), engine_id)
) WITH CLUSTERING ORDER BY (engine_id DESC);

CREATE TABLE bde_twitter.twitter_engine_per_post (
  engine_type text,
  engine_id bigint,
  post_id bigint,
  primary key ((engine_type), engine_id, post_id)
) WITH CLUSTERING ORDER BY (engine_id DESC, post_id DESC);