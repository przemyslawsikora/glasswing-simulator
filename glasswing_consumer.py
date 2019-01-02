from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'measurements',
    bootstrap_servers=['192.168.0.100:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    try:
        print(str(message))
    except Exception as e:
        print(e)
