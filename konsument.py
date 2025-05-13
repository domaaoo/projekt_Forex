import json
from kafka import KafkaConsumer
from datetime import datetime, timedelta

# Konfiguracja Kafki
KAFKA_BROKER = "localhost:9092" # Użyj adresu swojego brokera Kafki
KAFKA_TOPIC = "eurusd_tick_data"
CONSUMER_GROUP_ID = "eurusd_ohlc_consumer_group"

# Globalne przechowalnie dla OHLC
# Klucz: początek interwału (datetime), Wartość: [lista cen w interwale]
ohlc_data_1s = {}
ohlc_data_3s = {}
ohlc_data_5s = {}

# Przechowalnie dla aktualnie budowanych słupków
# Klucz: interwał (int), Wartość: {'start_time': datetime, 'prices': [float]}
current_bars = {
    1: {'start_time': None, 'prices': []},
    3: {'start_time': None, 'prices': []},
    5: {'start_time': None, 'prices': []}
}

def get_price_type_from_user():
    """Pyta użytkownika o typ ceny do analizy OHLC."""
    while True:
        choice = input("Wybierz typ ceny dla OHLC (ask/bid): ").strip().lower()
        if choice in ["ask", "bid"]:
            return choice
        else:
            print("Nieprawidłowy wybór. Wpisz 'ask' lub 'bid'.")

PRICE_TYPE_FOR_OHLC = "" # Zostanie ustawione po uruchomieniu

def make_decision(tick_data):
    """Placeholder dla reguły decyzyjnej."""
    # Tutaj w przyszłości można zaimplementować logikę kupna/sprzedaży
    # Na przykład:
    # if tick_data[PRICE_TYPE_FOR_OHLC] < some_moving_average and tick_data['ask_volume'] > some_threshold:
    #     print(f"SYGNAŁ KUPNA: {tick_data}")
    # else:
    #     print(f"Analiza ticka: {tick_data[PRICE_TYPE_FOR_OHLC]}")
    pass # Na razie nie robimy nic

def calculate_ohlc(prices_list):
    """Oblicza OHLC na podstawie listy cen."""
    if not prices_list:
        return None, None, None, None
    open_price = prices_list[0]
    high_price = max(prices_list)
    low_price = min(prices_list)
    close_price = prices_list[-1]
    return open_price, high_price, low_price, close_price

def get_bar_start_time(tick_timestamp, interval_seconds):
    """Oblicza czas rozpoczęcia interwału dla danego ticka i interwału."""
    if interval_seconds == 1:
        return tick_timestamp.replace(microsecond=0)
    elif interval_seconds == 3:
        return tick_timestamp.replace(second=tick_timestamp.second - (tick_timestamp.second % 3), microsecond=0)
    elif interval_seconds == 5:
        return tick_timestamp.replace(second=tick_timestamp.second - (tick_timestamp.second % 5), microsecond=0)
    return None

def process_tick_for_ohlc(tick_timestamp, price, interval_seconds):
    """Przetwarza tick dla danego interwału OHLC."""
    global current_bars

    bar_aggregator = current_bars[interval_seconds]
    current_bar_start_time = get_bar_start_time(tick_timestamp, interval_seconds)

    if bar_aggregator['start_time'] is None: # Pierwszy tick dla tego interwału
        bar_aggregator['start_time'] = current_bar_start_time
        bar_aggregator['prices'].append(price)
    elif current_bar_start_time > bar_aggregator['start_time']: # Nowy słupek
        # Oblicz i wyświetl OHLC dla zakończonego słupka
        o, h, l, c = calculate_ohlc(bar_aggregator['prices'])
        if o is not None:
            print(f"OHLC-{interval_seconds}s ({PRICE_TYPE_FOR_OHLC.upper()}) | Time: {bar_aggregator['start_time']} | O: {o:.5f} H: {h:.5f} L: {l:.5f} C: {c:.5f}")
        
        # Rozpocznij nowy słupek
        bar_aggregator['start_time'] = current_bar_start_time
        bar_aggregator['prices'] = [price]
    else: # Ten sam słupek, dodaj cenę
        bar_aggregator['prices'].append(price)


def create_consumer():
    """Tworzy i zwraca instancję KafkaConsumer."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest', # Zacznij odczyt od najwcześniejszych wiadomości
            group_id=CONSUMER_GROUP_ID,   # ID grupy konsumentów
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Konsument połączony z Kafka broker: {KAFKA_BROKER}, temat: {KAFKA_TOPIC}, grupa: {CONSUMER_GROUP_ID}")
        return consumer
    except Exception as e:
        print(f"Błąd podczas łączenia konsumenta z Kafką: {e}")
        return None

def consume_data(consumer):
    """Odbiera dane z Kafki i przetwarza je."""
    print(f"Oczekiwanie na wiadomości z tematu: {KAFKA_TOPIC}...")
    print(f"Dane OHLC będą generowane dla ceny: {PRICE_TYPE_FOR_OHLC.upper()}")
    try:
        for message in consumer:
            tick_data = message.value
            # print(f"Odebrano: {tick_data}") # Odkomentuj dla debugowania

            try:
                # Konwersja timestampu z ISO formatu
                tick_timestamp_str = tick_data.get("timestamp")
                if not tick_timestamp_str:
                    print(f"Brak timestampu w wiadomości: {tick_data}")
                    continue
                
                tick_datetime = datetime.fromisoformat(tick_timestamp_str)
                
                # Wybór ceny (Ask/Bid) na podstawie decyzji użytkownika
                price_to_use = tick_data.get(PRICE_TYPE_FOR_OHLC)
                if price_to_use is None:
                    print(f"Brak ceny '{PRICE_TYPE_FOR_OHLC}' w wiadomości: {tick_data}")
                    continue

                # 1. Wywołanie (placeholderowej) reguły decyzyjnej
                make_decision(tick_data)

                # 2. Agregacja OHLC
                process_tick_for_ohlc(tick_datetime, float(price_to_use), 1) # 1-sekundowy
                process_tick_for_ohlc(tick_datetime, float(price_to_use), 3) # 3-sekundowy
                process_tick_for_ohlc(tick_datetime, float(price_to_use), 5) # 5-sekundowy

            except json.JSONDecodeError:
                print(f"Błąd dekodowania JSON: {message.value}")
            except KeyError as ke:
                print(f"Brakujący klucz w wiadomości: {ke} w {tick_data}")
            except ValueError as ve:
                print(f"Błąd wartości podczas przetwarzania ticka: {ve} w {tick_data}")
            except Exception as e:
                print(f"Nieoczekiwany błąd przetwarzania wiadomości {tick_data}: {e}")
                
    except KeyboardInterrupt:
        print("Konsument zatrzymany przez użytkownika.")
    finally:
        print("Zamykanie konsumenta...")
        if consumer:
            consumer.close()
        print("Konsument zamknięty.")

if __name__ == "__main__":
    PRICE_TYPE_FOR_OHLC = get_price_type_from_user()
    
    consumer_instance = create_consumer()
    if consumer_instance:
        consume_data(consumer_instance)