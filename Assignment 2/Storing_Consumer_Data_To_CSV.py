import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

API_KEY = 'XSHTVVBNCEVBO2YV'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'IYfZ7BWy2ZB4t9S2xb/rmLNHfnvSIWiGq2CQvIdCPDhuyo6kNjWJLzyM21g7scvY'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'FWRF2O7LBGMOS2LB'
SCHEMA_REGISTRY_API_SECRET = 'uwW+RmWq5k49ic4sV8Z15FahqTuxCVn8XKHDhB3y9ap3wh5enSJF7VNbulZUZZbc'

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,
            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
            }


class Order:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_order(data: dict, ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def makeCSV(jsonData, w):
    """
    :param jsonData:
    :param w:
    CONVERT JSON DATA INTO CSV
    """
    rowList = []
    for col in jsonData:
        rowList.append(col)
    w.writerow(rowList)


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    my_schema = schema_registry_client.get_latest_version("restaurant-take-away-data-value").schema.schema_str
    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"}
    )

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if order is not None:
                print("User record {}: order: {} from {} partition and from {} offset\n"
                        .format(msg.key(), order, msg.partition(), msg.partition(), msg.offset()))

                makeCSV(order.record.values(), w)
        except KeyboardInterrupt:
            break
    consumer.close()


main("restaurant-take-away-data")