from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='enricher_group_v2', # Zmiana grupy, aby przeczytać dane od nowa po poprawce
    auto_offset_reset='earliest'
)

print("Rozpoczynam analizę ryzyka transakcji (odporną na błędy)...")

for message in consumer:
    tx = message.value
    
    # 1. Zabezpieczenie: sprawdź czy tx nie jest None i czy jest słownikiem
    if not isinstance(tx, dict):
        print(f"Pominięto niepoprawny format wiadomości: {tx}")
        continue

    amount = tx.get('amount', 0)
    
    # 2. Logika ryzyka
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    tx['risk_level'] = risk_level
    
    # 3. Bezpieczne wyciąganie ID
    # Jeśli 'id' nie istnieje, wstawi 'Unknown'
    tx_id = tx.get('id', 'Unknown')
    
    print(f"Transakcja: {tx_id} | Kwota: {amount} | Ryzyko: {tx['risk_level']}")
