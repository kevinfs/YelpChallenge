from pyspark.sql import SparkSession,SQLContext,Row
from pyspark import SparkContext
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
sc = SparkContext(appName="Workshop")
sqlContext=SQLContext(sc)

#Read Json File : business, user & review
business = spark.read.json("yelp_academic_dataset_business.json")
user = spark.read.json("yelp_academic_dataset_user.json")
review = spark.read.json("yelp_academic_dataset_review.json")

#GraphFrames

#Create Dataframe

table_business = business.map(lambda p: Row(id = p[0],business_stars = p[1]))
df_business = sqlContext.createDataFrame(table_business)

table_user = user.map(lambda p: Row(id_user = p[0],friends = p[]))
df_user = sqlContext.createDataFrame(table_user)

table_review = review.map(lambda p: Row(review_id = p[0],user_id = p[],business_id= p[],stars = p[]))
df_review = sqlContext.createDataFrame(table_review)

#Put dataframe into table

#friends_link = sqlContext.registerDataFrameAsTable(dataframe,"friends_link")
#id_user = sqlContext.sql("SELECT source AS id from friends_link")

