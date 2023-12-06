from confluent_kafka import Producer

BOOTSTRAP_SERVER = 'localhost:30756'

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER
}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

topic = 'test-topic'
producer.produce(topic, key="key", value="value", callback=acked)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
# producer.poll(1)
producer.flush()