from pyspark import SparkContext
from pyspark.sql import SQLContext


#Creation of the spark context
sc = SparkContext(appName="Big Data Workshop")
sqlContext = SQLContext(sc)

business = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_business.json")
review = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_review.json")
user = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_user.json")

business.cache()
review.cache()
user.cache()

'''
business.show()
review.show()
user.show()
'''

business.registerTempTable("business")
review.registerTempTable("review")
user.registerTempTable("user")

business_basic = sqlContext.sql("SELECT business_id, stars FROM business")
review_basic = sqlContext.sql("SELECT review_id,user_id, business_id, stars, date FROM review")
user_basic = sqlContext.sql("SELECT user_id, average_stars FROM user")

business_basic.registerTempTable("business_basic")
review_basic.registerTempTable("review_basic")
user_basic.registerTempTable("user_basic")

review_by_user = review_basic.join(user_basic, review_basic.user_id == user_basic.user_id)


