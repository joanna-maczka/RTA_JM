from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    # Pobieramy dane transakcji ze słownika message.value
    data = message.value
    
    # Sprawdzamy, czy kwota jest większa niż 3000
    if data['amount'] > 3000:
        # Jeśli tak, wypisujemy sformatowany komunikat
        print(f"ALERT: {data['tx_id']} | {data['amount']} PLN | {data['store']} | {data['category']}")
