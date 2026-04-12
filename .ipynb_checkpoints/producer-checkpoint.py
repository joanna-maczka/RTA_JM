from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tx_counter = 1

def generate_transaction():
    global tx_counter
    # Generowanie unikalnego ID transakcji (np. TX0001, TX0002)
    tx_id = f"TX{tx_counter:04d}"
    tx_counter += 1
    
    # Losowanie u01 do u20
    user_id = f"u{random.randint(1, 20):02d}"
    
    # Losowa kwota od 5.0 do 5000.0 (zaokrąglona do 2 miejsc po przecinku)
    amount = round(random.uniform(5.0, 5000.0), 2)
    
    # Losowe miasto i kategoria
    store = random.choice(["Warszawa", "Kraków", "Gdańsk", "Wrocław"])
    category = random.choice(["elektronika", "odzież", "żywność", "książki"])
    
    # Aktualny czas w formacie ISO
    timestamp = datetime.now().isoformat()
    
    # Zwrócenie słownika
    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "store": store,
        "category": category,
        "timestamp": timestamp
    }

# Pętla: generuj, wyślij, wypisz, poczekaj 1s
while True:
    transaction = generate_transaction()
    producer.send('transactions', value=transaction)
    print(f"Wysłano: {transaction}")
    time.sleep(1)
