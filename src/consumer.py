import sys, json, os
from kafka import KafkaConsumer
from kafka import TopicPartition
from report_pb2 import *



#sys.argv 0 is script name
partitions = sys.argv[1:]
#Convert array into int
partitions = [int(n) for n in partitions] 

broker = "localhost:9092"

consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.assign([TopicPartition("temperatures", p) for p in partitions])

offsets = {}

for p_num in partitions:
    # if a parition-N file exists, load its offset, else set offset to 0 for that partition
    if (os.path.isfile(f"/src/partition-{p_num}.json")):
        with open(f"/src/partition-{p_num}.json", "r") as file:
            data = json.load(file)
        offsets[TopicPartition(topic='temperatures',partition=p_num)] = data["offset"]
    else:
        offsets[TopicPartition(topic='temperatures',partition=p_num)] = 0 # seek to offset 0
# print(offsets)

# Do the seeking here
for tp, position in offsets.items():
    consumer.seek(tp, position)

stats = {}

while True: #Make infinite while True
    batch = consumer.poll(1000)
    for tp, messages in batch.items():
        p = tp.partition
        for msg in messages:
            msg = Report.FromString(msg.value)
            if p not in stats:
                stats[p] = {}
            if not msg.station_id in stats[p]:
                stats[p][msg.station_id] = {"count": 0, "sum": 0, "avg": None, "start": msg.date, "end": None}
            stats[p][msg.station_id]["count"] += 1
            stats[p][msg.station_id]["sum"] += msg.degrees
            stats[p][msg.station_id]["avg"] = stats[p][msg.station_id]["sum"]/stats[p][msg.station_id]["count"]
            stats[p][msg.station_id]["end"] = msg.date

    positions = {}
    for tp in consumer.assignment():
        positions[tp.partition] = consumer.position(tp)
    # print(positions)

#Write atomically
    for part in stats:
        data_to_dump = {"offset": positions[part]}
        data_to_dump.update(stats[part])

        path = f"/src/partition-{part}.json"
        path2 = path+".tmp"
        with open(path2, "w") as outfile:
            json.dump(data_to_dump, outfile, indent=4)
            os.rename(path2, path)