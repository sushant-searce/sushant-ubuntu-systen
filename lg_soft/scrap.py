from faker import Faker
from faker.providers import phone_number
fake = Faker()

for i in range(2000):
    name_text = fake.name().split();
    city_text = fake.city().split();
    address_text = fake.address().split();
    print("db.tutorial.insert({'id' : '"+str(i)+"'},{'name' : '"+name_text[0]+"'})")
    #print("insert into source.student values ('"+str(i)+"', '"+name_text[0]+"', '"+name_text[1]+"', '"+name_text[1]+"', '"+name_text[1]+"');")
