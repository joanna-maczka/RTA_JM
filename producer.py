from kafka import KafkaProducer
import json, random, time
from datetime import datetime
import uuid

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        "tx_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 60),
        "amount": round(random.uniform(5, 5000), 2),
        "store": random.choice(["Amazon", "Walmart", "Target", "eBay"]),
        "category": random.choice(["electronics", "clothing", "food", "books"]),
        "timestamp": datetime.utcnow().isoformat()
    }

# pętla generująca 1 transakcję na sekundę
while True:
    tx = generate_transaction()
    producer.send('transactions', tx)

    print(f"TX: {tx['tx_id']} | {tx['user_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")

    time.sleep(1)
