import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext, SQLContext

conf = SparkConf().setAppName("IM Funds Data Analysis")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)


## For Mutual Fund Data
mutual_funds_data = sc.textFile("~/BigData/Research/BNY-Code-Challenge/data/Fund_Data_1.txt").map(lambda d:(d.split("\t"))).toDF(["SHARECLASS_ID","SHARECLASS_NAME","FUND_NAME","SHARE_CLASS_COUNTER","FUND_FAMILY_NAME","MSTAR_CATEGORY_NAME","MSTAR_GLOB_CATEGORY_NAME","MSTAR_BROAD_CATEGORY","COMMON_SOURCE_SYSTEM","RETURN_NET_1M","RETURN_NET_2M","RETURN_NET_3M","RETURN_NET_6M","RETURN_NET_1Y","RETURN_NET_2Y","RETURN_NET_3Y","RETURN_NET_4Y","RETURN_NET_5Y"])
mutual_funds_data.createOrReplaceTempView("mutual_funds_data")
sqlContext.sql("select FUND_NAME,avg(RETURN_NET_5Y) from mutual_funds_data where SHARE_CLASS_COUNTER >1 group by FUND_NAME").show()


## For ETF Data
et_funds_data = sc.textFile("~/BigData/Research/BNY-Code-Challenge/data/Fund_Data_2.txt").map(lambda d:(d.split("\t"))).toDF(["SHARECLASS_ID","SHARECLASS_NAME","FUND_NAME","CATEGORY","SHARPE_RATIO_1Y","SHARPE_RATIO_3Y","SHARPE_RATIO_5Y","BETA_1Y","BETA_3Y","BETA_5Y","INFO_RATIO_3Y","INFO_RATIO_5Y"])
et_funds_data.createOrReplaceTempView("et_funds_data")
sqlContext.sql("select FUND_NAME,avg(INFO_RATIO_5Y) from et_funds_data group by FUND_NAME having count(SHARECLASS_ID) > 1").show()


'''
Select mf.FUND_NAME,avg(ef.INFO_RATIO_5Y)
from mutual_funds_data mf join  et_funds_data ef
on mf.SHARECLASS_ID = ef.SHARECLASS_ID
Where mf.SHARE_CLASS_COUNTER > 1
group by mf.FUND_NAME;
'''
result = sqlContext.sql("Select mf.FUND_NAME,avg(ef.INFO_RATIO_5Y) from mutual_funds_data mf join  et_funds_data ef on mf.SHARECLASS_ID = ef.SHARECLASS_ID Where mf.SHARE_CLASS_COUNTER > 1 group by mf.FUND_NAME")

result.coalesce(1).write.format("com.databricks.spark.csv").options(delimiter=',').option("header","true").save("~/BigData/Research/BNY-Code-Challenge/data/result")

