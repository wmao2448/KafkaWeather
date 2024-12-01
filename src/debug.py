from kafka import KafkaConsumer
from report_pb2 import *


broker = "localhost:9092"

consumer = KafkaConsumer(bootstrap_servers=[broker], auto_offset_reset="latest", group_id="debug")
consumer.subscribe(["temperatures"])

while True:

    dict_msg = {
        "station_id": None,
        "date": None,
        "degrees": None,
        "partition": None
    }

    batch = consumer.poll(1000)
    for topic_partition, messages in batch.items():
        dict_msg["partition"] = topic_partition.partition
        for msg in messages:
            msg = Report.FromString(msg.value)
            dict_msg["station_id"] = msg.station_id
            dict_msg["date"] = msg.date
            dict_msg["degrees"] = msg.degrees
        print(dict_msg)