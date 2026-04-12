from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)

print("Wykrywacz anomalii uruchomiony. Nasłuchuję...")

for message in consumer:
    transaction = message.value
    user_id = transaction['user_id']

    current_time = datetime.fromisoformat(transaction['timestamp'])
 
    user_history[user_id].append(current_time)
    
    recent_transactions = [
        t for t in user_history[user_id] 
        if (current_time - t).total_seconds() <= 60
    ]
    
    user_history[user_id] = recent_transactions
    
    if len(recent_transactions) > 3:
         print(f"ALERT! Użytkownik {user_id} wykonał {len(recent_transactions)} transakcje w ciągu ostatnich 60 sekund!")
