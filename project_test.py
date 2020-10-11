from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import ast

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, VectorAssembler, CountVectorizer, IDF, IDFModel, CountVectorizerModel, StringIndexer
from nltk.stem import PorterStemmer
import numpy as np
from functools import reduce
from datetime import datetime
from google.cloud import storage
from io import BytesIO
from tensorflow.python.lib.io import file_io
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.ml.linalg import Vectors


# Connect Kafka
kafka_topic = 'from-pubsub'
zk = '10.180.0.2:2181'
app_name = 'from-pubsub-app'
sc = SparkContext(appName="KafkaPubsub")
ssc = StreamingContext(sc, 30)
kafkaStream = KafkaUtils.createStream(ssc, zk, app_name, {kafka_topic: 1})


# Function to get lazy instance of pyspark
def getSparkSessionInstance(sparkConf):
	if ("sparkSessionSingletonInstance" not in globals()):
		globals()["sparkSessionSingletonInstance"] = SparkSession \
			.builder \
			.config(conf=sparkConf) \
			.getOrCreate()
	return globals()["sparkSessionSingletonInstance"]


# Function to act on every row rdd
def process(time, rdd):

	if not rdd.isEmpty():
		print('Processing Row')
		spark = getSparkSessionInstance(rdd.context.getConf())

		# Convert rdd to a row rdd
		rowVals = rdd.map(lambda row: Row(review_id=row['review_id'], user_id=(row['user_id']),
			business_id=row['business_id'], stars=float(row['stars']), useful=float(row['useful']), 
			funny=float(row['funny']), cool=float(row['cool']), text=row['text']+row['text2'], date=row['date']))

		# Convert row rdd to Spark Dataframe
		raw_data = spark.createDataFrame(rowVals)
		
		bucket = 'bdl_project'
		
		# Loading and applying all pre-trained models
		indexer = PipelineModel.load('gs://' + bucket + '/indexer')
		raw_data = indexer.transform(raw_data)
		raw_data = raw_data.drop('business_id', 'user_id')

		# Perform tokenization and stopword removal
		pre_process = PipelineModel.load('gs://' + bucket + '/pre_process')
		data = pre_process.transform(raw_data)
		data = data.drop('text', 'tokenized_text')

		# Perform stemming
		ps = PorterStemmer()
		stemmer = udf(lambda text: [ps.stem(token) for token in text], ArrayType(StringType()))
		data = data.withColumn('stemmed_text', stemmer('filtered_text'))
		data = data.drop('filtered_text')

		# Obtain tf-idf
		cv_model = CountVectorizerModel().load('gs://' + bucket + '/cv_model')  
		cvData = cv_model.transform(data)
		idf_model = IDFModel().load('gs://' + bucket + '/idf_model')
		tfidfData = idf_model.transform(cvData)

		# Perform GloVe Feature Engineering
		f = BytesIO(file_io.read_file_to_string('gs://bdl_project/glove.npy', binary_mode=True))
		glove = np.load(f)
		
		# Get vocabulary and word index
		vocab = cv_model.vocabulary
		ind2word = dict(zip(range(len(vocab)), vocab))
		vocab_len = len(vocab)
		
		# Feature engineering to obtain sentence representation using weighted glove representations
		glove_cols = tfidfData.rdd.map(lambda x:(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8],x[9],Vectors.dense(x[10].dot(glove)/x[10].dot(np.ones(vocab_len))))).toDF()

		# Rename and clean test data
		oldColumns = glove_cols.schema.names
		newColumns = tfidfData.schema.names[:-1] + ["glove_emb"]
		test = reduce(lambda glove_cols, idx: glove_cols.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), glove_cols)
		test = test.drop("lemmatized_text", "tf")

		# Feature engineering on date columns
		client = storage.Client()
		bucket_obj = client.get_bucket('project-bdl')
		blob = bucket_obj.get_blob('min_date.txt')
		min_date = datetime.strptime(blob.download_as_string().decode('utf-8')[:-7], '%Y-%m-%d %H:%M:%S')
		
		delta_days = udf(lambda x: int((min_date-datetime.strptime(x, '%Y-%m-%d %H:%M:%S')).days))
		test = test.withColumn('delta_days', delta_days('date'))
		test = test.drop('date')
		test = test.withColumn("delta_days", test["delta_days"].cast(IntegerType()))
		
		assembler = VectorAssembler(inputCols = ['cool','funny','useful','glove_emb','delta_days','business_ind','user_ind'], outputCol="features")
		test = assembler.transform(test)
		test = test.withColumnRenamed('stars', 'label')
		lr_model = LogisticRegressionModel.load('gs://bdl_project/logreg_model')
		result = lr_model.transform(test)
		result.show()
		result.select('review_id', 'prediction', 'label').show()


# vals = kafkaStream.map(lambda line: ast.literal_eval(line[1].decode('utf-8')))
vals = kafkaStream.map(lambda line: ast.literal_eval(line[1]))
# Operate on each RDD of dstream
vals.foreachRDD(process)

# Start processing
ssc.start()
ssc.awaitTermination()