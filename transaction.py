import uuid

from faker import Faker

TRANSACTION_SCHEMA = """
{
    "name": "Transaction",
    "type": "record",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "user_id",
            "type": "long"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "merchant_name",
            "type": "string"
        },
        {
            "name": "latitude",
            "type": "float"
        },
        {
            "name": "longitude",
            "type": "float"
        }
    ]
}
"""


class Transaction(object):
    def __init__(self, user_id, amount, merchant_name, latitude, longitude):
        self.id = str(uuid.uuid4().hex)
        self.user_id = user_id
        self.amount = amount
        self.merchant_name = merchant_name
        self.latitude = latitude
        self.longitude = longitude

    def __str__(self):
        return (f'Transaction: {self.id}, {self.user_id}, {self.amount}, {self.merchant_name}, {self.latitude}, '
                f'{self.longitude}')

    def __repr__(self):
        return self.__str__()

    @classmethod
    def get_fake(cls):
        fake = Faker()
        return Transaction(
            fake.random.randint(0, 1000),
            float(fake.random.randint(0, 100_000_000)) / 100,
            fake.company(),
            float(fake.random.randint(-1800, 1800)) / 10,
            float(fake.random.randint(-900, 900)) / 10,
        )

