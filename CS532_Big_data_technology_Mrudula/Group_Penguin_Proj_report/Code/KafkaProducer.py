from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import csv
import json

# Open the CSV  
f = open( '/home/cloudera/job_skills.csv', 'rU' )  
# Change each fieldname to the appropriate field name. I know, so difficult.  

reader = csv.DictReader( f, fieldnames = ( "company","title","category","location","responsibility","minimum","prefer" ))


# Kafka config
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic = "jobs_python"
# Parse the CSV into JSON 
for row in reader:
    message = json.dumps(row)
    producer.send(topic, message)
    time.sleep(1)