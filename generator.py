import time
from multiprocessing import Process

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField, IntegerSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from customer import Customer, CUSTOMER_SCHEMA

BOOTSTRAP_SERVER = 'localhost:30756'
SCHEMA_REGISTRY = 'http://localhost:8081'
CUSTOMERS_TOPIC = 'customers'

CUSTOMERS_PER_SECOND = 1

KAFKA_CONF = {
    'bootstrap.servers': BOOTSTRAP_SERVER
}

SCHEMA_REGISTRY_CONF = {
    'url': SCHEMA_REGISTRY
}


def customers_generator_process(kafka_conf, schema_registry_conf, customers_topic, customer_schema,
                                customers_per_second):
    producer = Producer(kafka_conf)
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    customer_avro_serializer = AvroSerializer(schema_registry_client, customer_schema)
    integer_serializer = IntegerSerializer()

    while True:
        customer = Customer.get_fake()

        producer.produce(
            topic=customers_topic,
            key=integer_serializer(customer.id),
            value=customer_avro_serializer(
                customer.__dict__,
                SerializationContext(customers_topic, MessageField.VALUE)
            )
        )

        time.sleep(1 / customers_per_second)


if __name__ == '__main__':
    customers_process = Process(
        target=customers_generator_process,
        args=(KAFKA_CONF, SCHEMA_REGISTRY_CONF, CUSTOMERS_TOPIC, CUSTOMER_SCHEMA, CUSTOMERS_PER_SECOND)
    )

    customers_process.start()
    customers_process.join()
