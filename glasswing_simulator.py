import math
import json
import time
from time import sleep
from json import dumps
from kafka import KafkaProducer

measurements_per_second = 2
producer = KafkaProducer(
    bootstrap_servers=['192.168.0.100:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    for i in range(measurements_per_second):
        data = {
            'source': '/sources/weather_stations/pl/katowice',
            'attribute': 'temperature',
            'datetime': '2019-01-02T08:12:35.000Z',
            'value': -1.5
        }
        msg = json.dumps(data)
        producer.send('measurements', value=msg)
    delay = math.ceil(time.time()) - time.time()
    print('Wait ' + str(delay * 1000) + ' ms')
    sleep(delay)
