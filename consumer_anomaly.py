from kafka import KafkaConsumer
import json
import time
from collections import defaultdict, deque

# Konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detector_group',
    auto_offset_reset='latest' # Skupiamy się na bieżących anomaliach
)

# Słownik przechowujący czasy transakcji dla każdego użytkownika
# Każdy wpis to kolejka (deque), która automatycznie usuwa stare dane
user_history = defaultdict(deque)

print("System wykrywania anomalii uruchomiony (3+ transakcje w 60s)...")

for message in consumer:
    tx = message.value
    if not isinstance(tx, dict) or 'user_id' not in tx:
        continue

    user_id = tx['user_id']
    current_time = time.time()  # Pobieramy aktualny czas systemowy
    
    # 1. Pobierz historię czasów dla tego użytkownika
    timestamps = user_history[user_id]
    
    # 2. Dodaj aktualny czas do historii
    timestamps.append(current_time)
    
    # 3. Usuń z historii wszystkie znaczniki starsze niż 60 sekund (czyszczenie okna)
    while timestamps and timestamps[0] < current_time - 60:
        timestamps.popleft()
    
    # 4. Sprawdź warunek anomalii
    # Jeśli w kolejce są więcej niż 3 wpisy, oznacza to 4+ transakcje w ciągu minuty
    if len(timestamps) > 3:
        print(f"!!! ALERT ANOMALII !!!")
        print(f"Użytkownik: {user_id} wykonał {len(timestamps)} transakcje w krótkim czasie!")
        print(f"Ostatnia transakcja: {tx.get('id')} | Kwota: {tx.get('amount')} PLN")
        print("-" * 30)
