from pyspark import SparkConf, SparkContext

# Recommendation system on Ecommerce data 

# Load events data from hdfs

events = spark.read.options(header=True,inferSchema=True).csv('/user/capstone/events.csv')

events.show(5)

events.count()


# Read item_properties_part1 data from hdfs

item1 = spark.read.options(header=True,inferSchema=True).csv('/user/capstone/item_properties_part1.csv')

item1.show(5)

item1.count()


# Read item_properties_part2 from hdfs

item2 = spark.read.options(header=True,inferSchema=True).csv('/user/capstone/item_properties_part2.csv')

item2.count()

item2.show(5)

# Union item1 and item2 data

item = item1.unionAll(item2)

item.count()

item.show(5)

# Select items only with categoryid

itemsWithCategory = item[item.property == 'categoryid']

itemsWithCategory.show(5)

itemsWithCategory.count()


# itemsWithCategory groupBy items with max timestamp

itemsWithCategoryidMaxTimestamp = itemsWithCategory.groupBy('itemid').max('timestamp')

itemsWithCategoryidMaxTimestamp.count()


# Rename the max(timestamp) as timestamp to merge in itemsWithCategory

item_time = itemsWithCategoryidMaxTimestamp.toDF('itemid','timestamp')

# Join the itemsWithCategory and item with max time

itemsWithMaxtimestamCategory = itemsWithCategory.join(item_time, ['itemid','timestamp'          ])

itemsWithMaxtimestamCategory.show(5)

itemsWithMaxtimestamCategory.count()


# Drop timestamp and transactionid as a part of cleaning NAs and noise coulmn

events_clean = events.drop('timestamp', 'transactionid')

events_clean.show(5)


# Join the itemsWithMaxtimestampCategory with events data

eventItemsWithMaxtimestamCategory = events_clean.join(itemsWithMaxtimestamCategory, ['itemid'])

eventItemsWithMaxtimestamCategory.count()


eventItemsWithMaxtimestamCategory.show(5)

# Give values to variable event-view=1, addtocart=2,transaction=3

view = eventItemsWithMaxtimestamCategory.replace({'view':'1'})

addtocart = view.replace({'addtocart':'2'})

CatVisitor = addtocart.replace({'transaction':'3'})

CatVisitor.show(5)


# Save final CalegoryVisitor file 

CatVisitor.coalesce(1).write.option('header','true').csv('/user/capstone/CatVisitor.csv')

# Rename the value variable as category and drop property column

CatVisitor_1 = CatVisitor.drop('property')

CatVisitor_1.show(2)

CatVisitor_2 = CatVisitor_1.toDF('itemid','visitorid','event','timestamp','categoryid')

CatVisitor_2.show(2)

# Extract columns visitorid, categoryid and event to create recommender matrix


CatVisitor_3 = CatVisitor_2['visitorid','categoryid','event']

CatVisitor_3.show(2)

# Drop duplicates-take distinct values

final = CatVisitor_3.distinct()

final.count()


# Save finalMarix into hdfs

final.coalesce(1).write.option('header','true').csv('/user/capstone/finalMatrix.csv')

# Load and parse the data finalMarix from hdfs

data = sc.textFile('/user/capstone/finalMatrix.csv')

type(data)

# Remove header from final Matrix

data.first()

header=data.first()

dataWithNoHeader = data.filter(lambda x: x!=header)

dataWithNoHeader.first()

# import
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

eventings = dataWithNoHeader.map(lambda x: x.split(',')).map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

type(eventings)

# Build the recommendation model using Alternating Least Squares

rank=10
numIterations=10
model = ALS.train(eventings, rank, numIterations)

type(model)

# Evaluate the model on training data

testdata = eventings.map(lambda p: (p[0], p[1]))

testdata.take(5)
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))

predictions.take(2)

ratesAndPreds = eventing.map(lambda r: ((r[0],r[1]),r[2])).join(predictions)

ratesAndPreds.take(2)

MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

print("Mean Squared Error = " + str(MSE))


# import 
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator

lines = spark.read.text('/user/capstone/finalMatrix.csv').rdd

type(lines)


lines.first()

header=lines.first()

lines_1 = lines.filter(lambda x: x!=header)

lines_1.first()

parts = lines_1.map(lambda row: row.value.split(','))

eventingRDD = parts.map(lambda p: Row(visitorId=int(p[0]), categoryId=int(p[1]),event=float(p[2])))

type(ratingsRDD)

eventings = spark.createDataFrame(eventingRDD)

type(eventingRDD)

# Split eventings data as training and test data

(training,test) = eventings.randomSplit([0.8,0.2])

training.count()

test.count()

# Build the recommendation model using ALS on the training data
#Set colstar strategy to drop NaN

als = ALS(maxIter=5, regParam=0.01, userCol="visitorId", itemCol="categoryId", ratingCol="event", coldStartStrategy="drop")

model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data

predictions = model.transform(test)

predictions.take(2)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="event",predictionCol="prediction")

rmse = evaluator.evaluate(predictions)

print("Root-mean-square error = " + str(rmse))

# Generate top 10 category recommendations for each user

userRecs = model.recommendForAllUsers(10)

userRecs.take(1)

# Generate top 10 user recommendations for each category

CategoryRecs = model.recommendForAllItems(10)

CategoryRecs.take(1)


# the end #


