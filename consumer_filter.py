from kafka import KafkaConsumer
import json

# Konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'  # Opcjonalne: czytaj od początku, jeśli brak zapisanych offsetów
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    tx = message.value  # Dane transakcji to słownik (dzięki json.loads)
    
    # Sprawdzenie warunku kwoty
    if tx.get('amount', 0) > 3000:
        # Wyciąganie danych do formatu ALERT
        tx_id = tx.get('id', 'N/A')
        amount = tx.get('amount', 0.0)
        city = tx.get('city', 'Nieznane')
        category = tx.get('category', 'Brak kategorii')
        
        print(f"ALERT: {tx_id} | {amount:.2f} PLN | {city} | {category}")
