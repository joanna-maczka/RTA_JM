from kafka import KafkaConsumer
from collections import Counter
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Nasze zmienne stanowe (pamięć konsumenta)
store_counts = Counter()
total_amount = {}
msg_count = 0

print("Nasłuchuję... Tabela zagregowanych danych pojawi się po zebraniu 10 transakcji.")

for message in consumer:
    transaction = message.value
    store = transaction['store']
    amount = transaction['amount']
    
    # 1. Zwiększamy licznik dla konkretnego sklepu (Counter robi to automatycznie)
    store_counts[store] += 1
    
    # 2. Dodajemy kwotę do całkowitej sumy danego sklepu
    # Jeśli sklepu nie ma jeszcze w słowniku, ustawiamy wartość początkową na 0.0
    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount
    
    msg_count += 1
    
    # 3. Co 10 wiadomości wypisujemy tabelę
    if msg_count % 10 == 0:
        print("\n" + "=" * 55)
        print(f"{'Sklep':<12} | {'Liczba':<8} | {'Suma (PLN)':<12} | {'Średnia (PLN)':<12}")
        print("-" * 55)
        
        # Pętla po wszystkich sklepach zapisanych w pamięci
        for s, count in store_counts.items():
            suma = total_amount[s]
            srednia = suma / count
            # :<12.2f oznacza: wyrównaj do lewej (zajmij 12 znaków), pokaż 2 miejsca po przecinku
            print(f"{s:<12} | {count:<8} | {suma:<12.2f} | {srednia:<12.2f}")
        
        print("=" * 55)
