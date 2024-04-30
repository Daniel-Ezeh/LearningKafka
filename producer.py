from kafka import KafkaProducer
from data import *
import json
import time

# def json_serializer(data): 
#     return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8'),
    partitioner = lambda y,z,a: 0  #This line helps us to send message to a specific partition.
)


if __name__ == '__main__':
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send('report', registered_user)
        time.sleep(20)
