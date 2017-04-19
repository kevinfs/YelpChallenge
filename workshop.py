from pyspark import SparkContext
from pyspark.sql import SQLContext


#Creation of the spark context
sc = SparkContext(appName="Big Data Workshop")
sqlContext = SQLContext(sc)

business = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_business.json")
review = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_review.json")
user = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_user.json")

business.cache()

business.show()
review.show()
user.show()

business.registerTempTable("business")
review.registerTempTable("review")
user.registerTempTable("user")


sc.stop()

