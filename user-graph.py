from pyspark import SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql.functions import lit
from graphframes import *


#Creation of the spark context
sc = SparkContext(appName="Big Data Workshop")
sqlContext = SQLContext(sc)

formatter = 'com.databricks.spark.csv'

# load the entire edge and node set into a Spark DataFrame
df = sqlContext.read.format(formatter).options(delimiter=' ', \
    header='false', inferSchema=True) \
    .load('user-graph.txt').withColumnRenamed( \
    '_c0', 'src').withColumnRenamed('_c1', 'dst')

#Add relationship to be more precise
#df_with_action = df.withColumn("relationship", lit("friend"))

#Register Data Frame as table
sqlContext.registerDataFrameAsTable(df, "user_graph")

#Vertices
v = sqlContext.sql("SELECT src AS id FROM user_graph")
v.show()

#Edge
e = sqlContext.sql("SELECT src ,dst FROM user_graph")
e.show()

# Create a GraphFrame
g = GraphFrame(v, e)


'''
#Top-k (present different k) nodes based on their ranking using PageRank
print "page rank"

#Only top 10 nodes
g.pageRank(resetProbability=0.15, tol=0.01).vertices.sort(
    	    'pagerank', ascending=False).show(10)

print "done"

#Top-k (present different k) nodes based on the number of triangles they participate into
print "number of triangles"

#Only top 10 nodes
triangleCount = g.triangleCount().run()
triangleCount.sort(desc("count")).limit(10).show()

print "done"
'''










