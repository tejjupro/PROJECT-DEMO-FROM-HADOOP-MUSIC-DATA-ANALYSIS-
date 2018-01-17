CREATE DATABASE IF NOT EXISTS project;

USE project;

CREATE TABLE users_artists
(
user_id STRING,
artists_array ARRAY<STRING>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '&';

LOAD DATA LOCAL INPATH '/home/cloudera/Assignment/musicProject/lookupfiles/user-artist.txt'
OVERWRITE INTO TABLE users_artists;

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/Assignment/musicProject/exportedata/userartists'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT user_id,artists FROM users_artists LATERAL VIEW explode(artists_array) a AS artists;
