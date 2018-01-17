import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType,StructField,StringType,NumericType,IntegerType,ArrayType}

import org.apache.spark.sql.functions._

val batid = sc.textFile("/user/cloudera/project/logs/current-batch.txt").map(x => x.toInt).toDF().first.getInt(0)


//Music Data
val data = sc.textFile("/user/cloudera/project/exportedata/enricheddata/000000_0")

val MDSchemaString = "user_id:string,song_id:string,artist_id:string,timestamp:string,start_ts:string,end_ts:string,geo_cd:string,station_id:string,song_end_type:Int,like:Int,dislike:Int,batchid:Int,status:string"

val MDdataSchema = StructType(MDSchemaString.split(",").map(fieldInfo => StructField(fieldInfo.split(":")(0), if(fieldInfo.split(":")(1).equals("string")) StringType else IntegerType, true)))

val MDrowRDD = data.map(_.split(",")).map(r => Row(r(0),r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8).toInt,r(9).toInt,r(10).toInt,r(11).toInt,r(12)))

val MusicDataDF =sqlContext.createDataFrame(MDrowRDD, MDdataSchema)

MusicDataDF.registerTempTable("Music_data")


//Subscribed Users
val data =sc.textFile("/user/cloudera/project/exportedata/subscribeduser/000000_0")

val SUSchemaString = "user_id:string,start_dt:string,end_dt:string"

val SUdataSchema = StructType(SUSchemaString.split(",").map(fieldInfo => StructField(fieldInfo.split(":")(0),if(fieldInfo.split(":")(1).equals("string")) StringType else IntegerType,true)))

val SUrowRDD = data.map(_.split(",")).map(r => Row(r(0),r(1),r(2)))

val SubscribedUsersDF = sqlContext.createDataFrame(SUrowRDD, SUdataSchema)

SubscribedUsersDF.registerTempTable("Music_SubscribedUsers")


//User Artists

val data =sc.textFile("/user/cloudera/project/exportedata/userartists/000000_0")

val UASchemaString = "user_id:string,artists:string"

val UAdataSchema = StructType(UASchemaString.split(",").map(fieldInfo => StructField(fieldInfo.split(":")(0),if(fieldInfo.split(":")(1).equals("string")) StringType else IntegerType,true)))

val UArowRDD = data.map(_.split(",")).map(r => Row(r(0),r(1)))

val UserArtistsDF = sqlContext.createDataFrame(UArowRDD, UAdataSchema)

UserArtistsDF.registerTempTable("Music_UserArtists")

val Top10Stations = sqlContext.sql(s"SELECT station_id,COUNT(DISTINCT song_id) AS total_distinct_songs_played, COUNT(DISTINCT user_id) AS Distinct_user_count, batchid FROM Music_data WHERE status='pass' AND batchid=$batid AND like=1 GROUP BY station_id,batchid ORDER BY total_distinct_songs_played DESC LIMIT 10");

Top10Stations.rdd.saveAsTextFile("/user/cloudera/project/output/top_10_stations")

val users_behavior =sqlContext.sql(s"SELECT CASE WHEN (subusers.user_id IS NULL OR CAST(music.timestamp AS DECIMAL(20,0)) > CAST(subusers.end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (subusers.user_id IS NOT NULL AND CAST(music.timestamp AS DECIMAL(20,0)) <= CAST(subusers.end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type, SUM(ABS(CAST(music.end_ts AS DECIMAL(20,0))-CAST(music.start_ts AS DECIMAL(20,0)))) AS duration, batchid FROM Music_Data music LEFT OUTER JOIN Music_SubscribedUsers subusers ON music.user_id=subusers.user_id WHERE music.status='pass' AND music.batchid=$batid GROUP BY CASE WHEN (subusers.user_id IS NULL OR CAST(music.timestamp AS DECIMAL(20,0)) > CAST(subusers.end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (subusers.user_id IS NOT NULL AND CAST(music.timestamp AS DECIMAL(20,0)) <= CAST(subusers.end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END,batchid")

users_behavior.rdd.saveAsTextFile("/user/cloudera/project/output/users_behavior")

val connected_artists = sqlContext.sql(s"SELECT ua.artists, COUNT(DISTINCT ua.user_id) AS user_count, md.batchid FROM Music_UserArtists ua INNER JOIN (SELECT artist_id, song_id, user_id,batchid FROM Music_Data WHERE status='pass' AND batchid=$batid ) md ON ua.artists=md.artist_id AND ua.user_id=md.user_id GROUP BY ua.artists,batchid ORDER BY user_count DESC LIMIT 10")

connected_artists.rdd.saveAsTextFile("/user/cloudera/project/output/connected_artists")

val top_10_royalty_songs = sqlContext.sql(s"SELECT song_id, SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration, batchid FROM Music_data WHERE status='pass' AND batchid=$batid AND (like=1 OR song_end_type=0) GROUP BY song_id, batchid ORDER BY duration DESC LIMIT 10")

top_10_royalty_songs.rdd.saveAsTextFile("/user/cloudera/project/output/top_10_royalty_songs")
s
val top_10_unsubscribed_users = sqlContext.sql(s"SELECT md.user_id, SUM(ABS(CAST(md.end_ts AS DECIMAL(20,0))-CAST(md.start_ts AS DECIMAL(20,0)))) AS duration FROM Music_data md LEFT OUTER JOIN Music_SubscribedUsers su ON md.user_id=su.user_id WHERE md.status='pass' AND md.batchid=$batid AND (su.user_id IS NULL OR (CAST(md.timestamp AS DECIMAL(20,0)) > CAST(su.end_dt AS DECIMAL(20,0)))) GROUP BY md.user_id ORDER BY duration DESC LIMIT 10")

top_10_unsubscribed_users.rdd.saveAsTextFile("/user/cloudera/project/output/top_10_unsubscribed_users")




