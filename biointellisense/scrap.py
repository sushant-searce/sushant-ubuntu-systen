from faker import Faker
from faker.providers import phone_number
fake = Faker()

for i in range(2000):
    name_text = fake.name().split();
    city_text = fake.city().split();
    address_text = fake.address().split();
    print("""{{"user_id":{}, "device_id":{}, "timestamp":{}, "level":"hourly"}}""".format(i, i+100, i+1624605237053))      
# {"user_id":1, "device_id":101, "timestamp":1624605237053, "level":""hourly}

