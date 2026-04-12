from kafka import KafkaConsumer
import json

# Inicjalizacja konsumenta z nowym group_id
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='risk_evaluation_group', # <-- Tutaj ustawiamy INNE group_id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Rozpoczynam ocenę ryzyka transakcji w czasie rzeczywistym...")

for message in consumer:
    # Pobranie danych transakcji do zmiennej
    transaction = message.value
    amount = transaction['amount']
    
    # Określanie poziomu ryzyka na podstawie kwoty
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
        
    # Wzbogacenie zdarzenia: dodajemy nowe pole do naszego słownika
    transaction['risk_level'] = risk_level
    
    # Wypisanie zmodyfikowanej transakcji
    print(f"[{risk_level}] Zaktualizowano transakcję: {transaction['tx_id']} - Kwota: {amount}")
