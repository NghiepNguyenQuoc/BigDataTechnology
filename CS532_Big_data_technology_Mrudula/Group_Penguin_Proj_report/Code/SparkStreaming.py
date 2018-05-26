import sys
import happybase
import json
import uuid
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Define host and table for hbase connection
server = 'localhost'
table_name = 'jobs'

#Columm on data set: "Company","Title","Category","Location","Responsibilities","Minimum Qualifications","Preferred Qualifications"


#Parse json and write to Hbase by using happybase https://happybase.readthedocs.io/en/latest/

def parser(x):

	#Get message from Kafka
	job = json.loads(x)
	
	#Connect to hbase
	table = happybase.Connection(server).table(table_name)
	
	#Put record to hbase
	table.put(str(uuid.uuid1()), {b'info:company': job['company'], b'info:title': job['title'],b'info:category': job['category'],b'info:location': job['location'],b'info:responsibility': job['responsibility'],b'info:minimum': job['minimum'],b'info:prefer': job['prefer']})	

	
#Spark streaming to Kafka server and parse messages

def main():
	sc = SparkContext(appName="PythonStreamingKafkaJobs")
	ssc = StreamingContext(sc,10)
	zookeeper = "localhost:2181"
	kafkaStream = KafkaUtils.createStream(ssc, zookeeper, "spark-streaming-consumer", {"jobs_python": 1})
	lines = kafkaStream.map(lambda x: x[1])
	lines.foreachRDD(lambda rdd: rdd.foreach(parser))
	ssc.start()
	ssc.awaitTermination()

#Main function
	
if __name__ == "__main__":
	
	main()
			
	


    