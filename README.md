<<<<<<< HEAD
# CS523-Final-Project

bin/kafka-server-start.sh config/server.properties;

bin/kafka-topics.sh --create --topic soccerTopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1;

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic soccerTopic;

CREATE EXTERNAL TABLE IF NOT EXISTS SoccerTweets(
    created_at STRING,
    username STRING,
    screen_name STRING,
    followers_count STRING,
    friends_count STRING,
    favorites_count STRING,
    lang STRING,
    source_data STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/soccerData';

My demo video:
https://web.microsoftstream.com/video/c04c74d1-bd26-4b00-beed-dd0b9c083cf5?list=studio