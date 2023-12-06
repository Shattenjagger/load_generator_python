import time
import os

from multiprocessing import Process

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField, IntegerSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from customer import Customer, CUSTOMER_SCHEMA
from transaction import Transaction, TRANSACTION_SCHEMA

BOOTSTRAP_SERVER = os.environ.get('G_BOOTSTRAP_SERVER', 'localhost:30756')
SCHEMA_REGISTRY = os.environ.get('G_SCHEMA_REGISTRY', 'http://localhost:8081')
CUSTOMERS_TOPIC = os.environ.get('G_CUSTOMERS_TOPIC', 'customers')
TRANSACTIONS_TOPIC = os.environ.get('G_TRANSACTIONS_TOPIC', 'transactions')

CUSTOMERS_PER_SECOND = float(os.environ.get('G_CUSTOMERS_PER_SECOND', 1))
TRANSACTIONS_PER_SECOND = float(os.environ.get('G_TRANSACTIONS_PER_SECOND', 1000))
TRANSACTIONS_PROCESSES = int(os.environ.get('G_TRANSACTIONS_PROCESSES', 20))

KAFKA_CONF = {
    'bootstrap.servers': BOOTSTRAP_SERVER
}

SCHEMA_REGISTRY_CONF = {
    'url': SCHEMA_REGISTRY
}


def transactions_generator_process(kafka_conf, schema_registry_conf, transactions_topic, transaction_schema,
                                   transactions_per_second):
    producer = Producer(kafka_conf)
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    transaction_avro_serializer = AvroSerializer(schema_registry_client, transaction_schema)
    string_serializer = StringSerializer('utf-8')

    while True:
        transaction = Transaction.get_fake()

        producer.produce(
            topic=transactions_topic,
            key=string_serializer(transaction.id),
            value=transaction_avro_serializer(
                transaction.__dict__,
                SerializationContext(transactions_topic, MessageField.VALUE)
            )
        )

        time.sleep(1 / transactions_per_second)


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

    processes = [
        Process(
            target=customers_generator_process,
            args=(KAFKA_CONF, SCHEMA_REGISTRY_CONF, CUSTOMERS_TOPIC, CUSTOMER_SCHEMA, CUSTOMERS_PER_SECOND)
        ),
    ]

    for i in range(TRANSACTIONS_PROCESSES):
        processes.append(
            Process(
                target=transactions_generator_process,
                args=(KAFKA_CONF, SCHEMA_REGISTRY_CONF, TRANSACTIONS_TOPIC, TRANSACTION_SCHEMA, TRANSACTIONS_PER_SECOND)
            )
        )

    for p in processes:
        p.start()

    for p in processes:
        p.join()
