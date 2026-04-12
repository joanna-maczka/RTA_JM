from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    # TWÓJ KOD
    # Zwróć słownik z polami: tx_id, user_id, amount, store, category, timestamp
    pass

# TWÓJ KOD
# Pętla: generuj transakcję, wyślij do tematu 'transactions', wypisz, sleep 1s
