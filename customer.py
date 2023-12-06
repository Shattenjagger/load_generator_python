from faker import Faker

class Customer(object):
    def __init__(self, name, country, birth_date, city, address, phone_number):
        self.name = name
        self.country = country
        self.birth_date = birth_date
        self.city = city
        self.address = address
        self.phone_number = phone_number

    def __str__(self):
        return f'Customer: {self.name}, {self.country}, {self.birth_date}, {self.city}, {self.address}, {self.phone_number}'

    def __repr__(self):
        return self.__str__()

fake = Faker()

customer = Customer(fake.name(), fake.country(), fake.date(), fake.city(), fake.address(), fake.phone_number())


print(customer)