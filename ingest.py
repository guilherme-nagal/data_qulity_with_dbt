from faker import Faker
import random
import string
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd

fake = Faker('pt_BR')
client = clickhouse_connect.get_client(host='localhost', port=8123, database='raw', username='dbt_user', password='bix123')

NUM_PRODUCTS = 1_000_000
NUM_CUSTOMERS = 1_000_000

def random_case(text):
    return text.lower() if random.random() < 0.5 else text.upper()

def maybe_mess_email(email):
    if random.random() < 0.2:
        return email.replace('@', '')
    elif random.random() < 0.4:
        return email.replace('.', ',')
    return email

def maybe_bad_price(price):
    if random.random() < 0.2:
        return str(price).replace('.', ',')
    elif random.random() < 0.1:
        return 'R$ ' + str(price)
    return str(price)

def add_extra_spaces(text):
    if text and random.random() < 0.3:
        return '  ' + text + '  '
    return text

def messy_text(text):
    if text is None:
        return None
    text = random_case(text)
    text = add_extra_spaces(text)
    if random.random() < 0.2:
        text = text.replace(' ', '')
    return text

# ---------- GERA PRODUTOS -----------
def generate_product(i):
    name = fake.word().capitalize()
    category = random.choice(['Eletrônicos', 'ELETRONICOS', 'livr os', 'Clothes', 'ROUPAS', ''])

    return (
        i,
        messy_text(name) if random.random() > 0.05 else '',
        fake.sentence() if random.random() > 0.2 else None,
        messy_text(category),
        maybe_bad_price(round(random.uniform(-50, 5000), 2)),
        random.choice([random.randint(-10, 1000), None]),
        fake.date_time_between(start_date='-3y', end_date='-1y'),
        fake.date_time_this_year(),
        random.choice([1, 0, None]),
        messy_text(random.choice(['Samsung', 'apple', 'LG', 'Nike', 'adidas', 'None'])),
        fake.ean(length=13) if random.random() > 0.1 else fake.ean(length=8),
        str(round(random.uniform(10, 5000), 2)) if random.random() > 0.2 else ''
    )

# ---------- GERA CLIENTES -----------
def generate_customer(i):
    name = fake.name()
    email = fake.email()
    birth = fake.date_of_birth(minimum_age=18, maximum_age=90)

    if random.random() < 0.1:
        birth = birth.strftime("%d/%m/%Y")
        
    # Extração de nomes
    first_name = name.split()[0] if name else None
    last_name = name.split()[-1] if name and len(name.split()) > 1 else None

    # Armazena os nomes limpos para outra tabela
    customer_names.append(((i+1)*2, first_name))
    customer_names.append(((i+1)*2-1, last_name))

    return (
        i,
        messy_text(name) if random.random() > 0.05 else None,
        maybe_mess_email(email),
        fake.phone_number() if random.random() > 0.1 else '',
        str(birth),
        fake.date_time_between(start_date='-5y', end_date='-2y'),
        fake.date_time_this_year(),
        random.choice([1, 0, None]),
        random.choice(['M', 'F', 'Outro', '', 'masculino', 'FEMININO', None]),
        fake.cpf().replace('.', '').replace('-', '') if random.random() > 0.2 else fake.cpf(),
        add_extra_spaces(fake.address().replace('\n', ', ')),
        fake.city(),
        random.choice(['SP', 'rj', 'BA', 'MG', 'SC', 'sp', '', None]),
        fake.postcode().replace('-', '') if random.random() > 0.1 else fake.postcode(),
        random.choice(['google', 'INSTAGRAM', 'Indicação', 'email', 'FACEBOOK', None])
    )

# -------- INSERIR DADOS ----------
customer_names = []
batch_products = [generate_product(i) for i in range(1, NUM_PRODUCTS + 1)]
batch_customers = [generate_customer(i) for i in range(1, NUM_CUSTOMERS + 1)]

client.insert('raw.products', batch_products)
client.insert('raw.customers', batch_customers)
client.insert('raw.customer_names', customer_names)

print("✅ Dados sujos inseridos diretamente no ClickHouse!")

