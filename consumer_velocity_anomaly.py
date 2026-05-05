from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_activity = defaultdict(deque)

WINDOW_SECONDS = 60
THRESHOLD = 3

def parse_time(ts):
    return datetime.fromisoformat(ts)

print("Monitoring: >3 transakcji / 60 sekund per user...")

for message in consumer:
    tx = message.value
    user_id = tx["user_id"]
    tx_time = parse_time(tx["timestamp"])

    history = user_activity[user_id]
    history.append(tx_time)

    while history and (tx_time - history[0]).total_seconds() > WINDOW_SECONDS:
        history.popleft()

    if len(history) > THRESHOLD:
        print(f"ALERT: user {user_id} | {len(history)} tx in 60s | last_tx={tx['tx_id']}")
