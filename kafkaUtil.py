from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, json):
    try:
        producer_instance.send(topic_name, json.encode('utf-8'))
        producer_instance.flush()
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
