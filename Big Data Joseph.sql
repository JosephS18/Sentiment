-- Athena Big Data queries
-- MJ Tela
-- Joseph Sabaybay

/************
 Raw dataset
 ************/

-- Load the dataset
select *
from blackfriday
limit 5;

-- count how many rows are in the dataset
select count(*)
from blackfriday;

-- put the tweet into lowercase
select *,
       lower(tweet)
from blackfriday limit 5;

-- get the length of the characters from the tweet
select *,
       lower(tweet) as lower_tweet,
       length(tweet) as num_char
from blackfriday limit 5;

-- convert from string to datetime
select *,
       date_parse(created_at, '%a %b %d %H:%i:%s +0000 %Y') as date_time
from(
    select *
    from blackfriday
    where length(created_at) = 30
) as Sub1;

-- Total follower count
select screen_name, followers_count
from black_friday
order by followers_count DESC;

-- Tweets per person
select screen_name, count(tweet) as total_tweets
from black_friday
group by screen_name
order by total_tweets DESC;


/******************
  Predictions table
 ******************/

-- Load the predictions dataset
SELECT * FROM "big_data"."blackfriday_pipe_parquet" limit 10;

-- Get the word count for the word cloud
select x word,
       count(x) as word_count
from blackfriday_pipe_parquet,
     unnest(filtered) as t(x)
group by x
order by x;

-- Convert created_at from string to datetime and use join to the predictions table
-- This will grab the datetime and the prediction of the of the tweet
select *,
       date_parse(created_at, '%a %b %d %H:%i:%s +0000 %Y') as date_time
from (
    select *
    from(
        select b.name, b.tweet, b.followers_count, b.location, p.sentiment, b.created_at, p.prediction
        from blackfriday_pipe_parquet as p
        join blackfriday as b on b.tweet = p.tweet
    ) as sub_1
    where length(created_at) = 30
) as sub_2