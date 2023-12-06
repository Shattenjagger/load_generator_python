import uuid

from faker import Faker

CUSTOMER_SCHEMA = """
{
    "name": "Customer",
    "type": "record",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "country",
            "type": "string"
        },
        {
            "name": "birth_date",
            "type": "string"
        },
        {
            "name": "favorite_number",
            "type": "long"
        },
        {
            "name": "address",
            "type": "string"
        },
        {
            "name": "phone_number",
            "type": "string"
        }
    ]
}
"""


class Customer(object):
    def __init__(self, name, country, birth_date, favorite_number, city, address, phone_number):
        self.id = str(uuid.uuid4().hex)
        self.name = name
        self.country = country
        self.birth_date = str(birth_date)
        self.favorite_number = favorite_number
        self.city = city
        self.address = address
        self.phone_number = phone_number

    def __str__(self):
        return (f'Customer: {self.id}, {self.name}, {self.country}, {self.birth_date}, {self.favorite_number}, '
                f'{self.city}, '
                f'{self.address}, {self.phone_number}')

    def __repr__(self):
        return self.__str__()

    @classmethod
    def get_fake(cls):
        fake = Faker()
        return Customer(fake.name(), fake.country(), fake.date(), fake.random.randint(0, 100),
                        fake.city(), fake.address(), fake.phone_number())
