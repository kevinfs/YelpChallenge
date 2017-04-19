from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions


#Creation of the spark context
sc = SparkContext(appName="Big Data Workshop")
sqlContext = SQLContext(sc)

business = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_business.json")
review = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_review.json")
user = sqlContext.read.json("../yelp_dataset/yelp_academic_dataset_user.json")

# friends = sqlContext.read.csv("../yelp_dataset/user-graph.txt")
formatter = 'com.databricks.spark.csv'
friends = sqlContext.read.format(formatter).options(delimiter=' ', \
    header='false', inferSchema=True) \
    .load('../yelp_dataset/user-graph.txt').withColumnRenamed( \
    '_c0', 'uid').withColumnRenamed('_c1', 'fid')

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
review_basic = sqlContext.sql("SELECT user_id AS rid, business_id, stars, date AS review_date FROM review")
user_basic = sqlContext.sql("SELECT user_id FROM user")

# A user and its reviews
user_reviews = review_basic.join(user_basic, review_basic.rid == user_basic.user_id).drop('rid')
user_reviews.show()

# A friend and its reviews
friend_reviews = user_reviews.withColumnRenamed('user_id', 'friend_id').withColumnRenamed('review_date', 'friend_review_date').withColumnRenamed('stars', 'friend_stars').withColumnRenamed('business_id', 'friend_business_id')
friend_reviews.show()

# Join everything
social_reviews = user_reviews.join(friends, user_reviews.user_id == friends.uid).join(friend_reviews, friends.fid == friend_reviews.friend_id).drop('uid', 'fid')
social_reviews = social_reviews.filter(social_reviews["business_id"] == social_reviews["friend_business_id"])
social_reviews = social_reviews.filter(social_reviews["review_date"] > social_reviews["friend_review_date"])
social_reviews.cache()
social_reviews.persist()
social_reviews.show()

social_groupby_business = social_reviews.groupBy(social_reviews.user_id, social_reviews.business_id).avg('friend_stars')

social_groupby_business.cache()
social_groupby_business.persist()
social_groupby_business.show()

social_groupby_business1 = social_reviews.select('user_id', 'business_id', 'stars').withColumnRenamed('user_id', 'uid').withColumnRenamed('business_id', 'bid')

social_groupby_business1.cache()
social_groupby_business1.persist()
social_groupby_business1.show()

social_groupby_business2 = social_groupby_business.join(social_groupby_business1, (social_groupby_business.user_id == social_groupby_business1.uid) & (social_groupby_business.business_id == social_groupby_business1.bid)).drop('uid').drop('bid').distinct()
social_groupby_business2.persist()
social_groupby_business2.show()


social_gap = social_groupby_business2.withColumn("friend_stars_gap", social_groupby_business2.stars-social_groupby_business2["avg(friend_stars)"])

social_gap.cache()
social_gap.persist()
social_gap.show()

social_groupby_users = social_gap.groupBy(social_gap.user_id).avg('friend_stars_gap').alias("social_gap_average")

social_groupby_users.cache()
social_groupby_users.persist()
social_groupby_users.show()

social_groupby_users_rounded = social_groupby_users.withColumn("rounded", functions.round("avg(friend_stars_gap)"))

social_groupby_users_rounded.cache()
social_groupby_users_rounded.persist()
social_groupby_users_rounded.show()

final_result = social_groupby_users_rounded.groupBy("rounded").count()

final_result.cache()
final_result.persist()
final_result.show()

# final_result.write.format("com.databricks.spark.csv").save("final_result")

# user_and_its_friends = user_reviews.join(friends, user_reviews.user_id == friends.uid).drop('uid')
# user_and_its_friends.show()


# user_friends_reviews = user_and_its_friends.join(user_reviews, user_reviews.user_id == user_and_its_friends.friend_id).drop('rid')

# friends_reviews = friends.join(user_reviews, friends.friend_id == user_reviews.user_id)

# friends_reviews.show()




