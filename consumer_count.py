from kafka import KafkaConsumer
from collections import Counter
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='stats_group',
    auto_offset_reset='earliest'
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Rozpoczynam zliczanie statystyk per sklep...")

for message in consumer:
    tx = message.value
    if not isinstance(tx, dict):
        continue

    # Pobieranie danych (z zabezpieczeniem przed brakiem kluczy)
    store = tx.get('store', 'Nieznany')
    amount = tx.get('amount', 0.0)

    # 1. Zwiększenie liczników
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0.0) + amount
    
    msg_count += 1

    # 3. Co 10 wiadomości wypisz tabelę
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (Wiadomości: {msg_count}) ---")
        print(f"{'Sklep':<20} | {'Liczba':<8} | {'Suma':<12} | {'Średnia':<10}")
        print("-" * 60)
        
        for store in sorted(store_counts.keys()):
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count
            print(f"{store:<20} | {count:<8} | {total:<12.2f} | {avg:<10.2f}")
        print("-" * 60)
