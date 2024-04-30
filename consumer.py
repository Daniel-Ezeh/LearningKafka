from kafka import KafkaConsumer
import json

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'report',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='consumer_group_A',
    )
    print("Starting the consumer")
    for msg in consumer:
        print(f'Registered user: \n{json.loads(msg.value)}')

