
import time
import weather
from report_pb2 import *
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1

try:
    admin_client.create_topics([NewTopic("temperatures", num_partitions=4, replication_factor=1)])
except TopicAlreadyExistsError:
    print("Topic still was not deleted")



print("Topics:", admin_client.list_topics())

#settings so that producer retries up to 10 times when send fails
#and send calls are not acknowledged until all in-sync replicas have recieved the data
producer = KafkaProducer(bootstrap_servers=[broker], acks="all", retries=10)

#Runs forever
for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    key = bytes(station_id, "utf-8")
    value = Report(date=date, degrees=degrees, station_id=station_id).SerializeToString()
    producer.send(topic="temperatures", value=value, key=key)