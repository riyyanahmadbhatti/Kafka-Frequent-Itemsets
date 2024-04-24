import json
from kafka import KafkaProducer
from time import sleep
from kafka.errors import KafkaTimeoutError

bootstrap_servers = ['localhost:9092']
topic1 = 'firsttopic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    try:
        sleep(2)
        with open('metasample.json', 'r') as file:
            for line in file:
                data = json.loads(line.strip())

                producer.send(topic1, value=data)
                print('Data sent to Kafka for topic 1:', data)

    except KafkaTimeoutError:
        print('KafkaTimeoutError: Failed to send data to Kafka. Retrying in 5 seconds...')
        sleep(5)
        continue
    except Exception as e:
        print(f'An error occurred: {e}')
        break
producer.close()

