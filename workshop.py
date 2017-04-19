from pyspark import SparkContext
from pyspark.sql import SQLContext


#Creation of the spark context
sc = SparkContext(appName="Big Data Workshop")
sqlContext = SQLContext(sc)

user = sqlContext.read.json("yelp_academic_dataset_tip.json")

user.show()

user.registerTempTable("user")

sc.stop()

