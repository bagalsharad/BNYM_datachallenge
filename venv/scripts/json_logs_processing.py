import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext, SQLContext

conf = SparkConf().setAppName("IM Funds Data Analysis")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)


from pyspark.sql.functions import explode, col

source_df = sc.read.option("multiline", "true").json("~/BigData/Research/BNY-Code-Challenge/data/results.json")
convert2rows = source_df.select(explode("results").alias("results"))

result_df = convert2rows.select(col("results.lastOpenedAt.iso").alias("lastOpenedAt"),
                                col("results.objectId").alias("objectId"),
                                col("results.appName").alias("appName"),
                                col("results.email").alias("email"),
                                col("results.localeIdentifier").alias("localeIdentifier"),
                                col("results.deviceType").alias("deviceType"),
                                col("results.timeZone").alias("timeZone"),
                                col("results.installationId").alias("installationId"),
                                col("results.deviceToken").alias("deviceToken"),
                                col("results.location.latitude").alias("latitude"),
                                col("results.location.longitude").alias("longitude")
                          )

result_df.write.format("csv").mode("append").option("delimiter", ",").save("~/BigData/Research/BNY-Code-Challenge/data/exercise_2_result")

result_df.createOrReplaceTempView("json_result")
sqlContext.sql("select timeZone, count(objectId) from json_result  group by timeZone").show()
sqlContext.sql("select timeZone,deviceType, count(objectId) from json_result  group by timeZone,deviceType").show()


'''
-- Schema for processed result.json 
-- Hive External Table 
Create Table IMDB.json_result(
    lastOpenedAt: string
    objectId: string
    appName: string
    email: string
    localeIdentifier: string
    deviceType: string
    timeZone: string
    installationId: string
    deviceToken: string
    latitude: double
    longitude: double )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINE TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION '~/Research/BNY-Code-Challenge/data/exercise_2_result';


Queries:
# get the number of objects per timezone
select timeZone, count(objectId) from json_result  group by timeZone;

select timeZone,deviceType, count(objectId) from json_result  group by timeZone,deviceType;


'''
