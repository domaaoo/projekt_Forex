import csv
import json
from datetime import datetime, timezone
import time
from kafka import KafkaProducer

# Konfiguracja Kafki
KAFKA_BROKER = "localhost:9092"  # Użyj adresu swojego brokera Kafki
KAFKA_TOPIC = "eurusd_tick_data"
INPUT_CSV_FILE = "EURUSD.csv"  # Upewnij się, że ten plik jest w tym samym katalogu

def parse_timestamp(ts_str):
    """Parsuje string z datą i czasem do obiektu datetime."""
    # Przykład formatu: 30.04.2025 00:00:00.081 GMT+0200
    # Usuwamy informację o strefie czasowej, jeśli datetime.strptime jej nie obsługuje bezpośrednio w prosty sposób
    # lub jeśli wszystkie dane są w tej samej strefie i interesują nas tylko różnice czasowe.
    # Dla uproszczenia i obliczania różnic, możemy zignorować część GMT.
    ts_part = ts_str.split(" GMT")[0]
    dt_obj = datetime.strptime(ts_part, "%d.%m.%Y %H:%M:%S.%f")
    # Można by dodać obsługę strefy czasowej, np. używając pytz, jeśli konieczne
    # Na potrzeby symulacji opóźnień, strefa nie jest krytyczna, jeśli jest spójna.
    return dt_obj

def create_producer():
    """Tworzy i zwraca instancję KafkaProducer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            retries=5, # Ilość ponowień w przypadku błędu wysyłania
            request_timeout_ms=30000 # Timeout dla żądania
        )
        print(f"Producent połączony z Kafka broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"Błąd podczas łączenia producenta z Kafką: {e}")
        return None

def stream_data(producer, file_path):
    """Odczytuje dane z pliku CSV i wysyła je do Kafki z odpowiednimi opóźnieniami."""
    previous_timestamp = None
    first_message = True

    print(f"Rozpoczynanie streamowania danych z pliku: {file_path} do tematu: {KAFKA_TOPIC}")

    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Pomiń nagłówek
            print(f"Nagłówek CSV: {header}")

            for i, row in enumerate(csv_reader):
                try:
                    local_time_str = row[0]
                    ask_price = float(row[1])
                    bid_price = float(row[2])
                    ask_volume = float(row[3])
                    bid_volume = float(row[4])

                    current_timestamp = parse_timestamp(local_time_str)

                    if not first_message and previous_timestamp:
                        delay = (current_timestamp - previous_timestamp).total_seconds()
                        if delay > 0:
                            # print(f"Oczekiwanie {delay:.3f} sekund...")
                            time.sleep(delay)
                        elif delay < 0:
                            print(f"Ostrzeżenie: Ujemne opóźnienie ({delay:.3f}s) dla wiersza {i+2}. Wysyłanie natychmiast.")


                    message = {
                        "timestamp": current_timestamp.isoformat(), # Używamy ISO format dla łatwiejszego parsowania
                        "ask": ask_price,
                        "bid": bid_price,
                        "ask_volume": ask_volume,
                        "bid_volume": bid_volume
                    }

                    producer.send(KAFKA_TOPIC, value=message)
                    # print(f"Wysłano wiadomość: {message}") # Odkomentuj dla debugowania

                    previous_timestamp = current_timestamp
                    if first_message:
                        first_message = False
                        print("Pierwsza wiadomość wysłana. Rozpoczynam symulację opóźnień.")
                    
                    if (i + 1) % 100 == 0: # Logowanie co 100 wiadomości
                        print(f"Wysłano {i+1} wiadomości.")


                except ValueError as ve:
                    print(f"Błąd parsowania danych w wierszu {i+2}: {row}. Błąd: {ve}")
                    continue # Pomiń błędny wiersz
                except IndexError:
                    print(f"Błąd indeksu w wierszu {i+2}: {row}. Prawdopodobnie niekompletny wiersz.")
                    continue

            producer.flush() # Upewnij się, że wszystkie wiadomości zostały wysłane
            print("Zakończono streamowanie wszystkich danych.")

    except FileNotFoundError:
        print(f"Błąd: Plik {file_path} nie został znaleziony.")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd podczas streamowania: {e}")

if __name__ == "__main__":
    producer_instance = create_producer()
    if producer_instance:
        try:
            stream_data(producer_instance, INPUT_CSV_FILE)
        except KeyboardInterrupt:
            print("Producent zatrzymany przez użytkownika.")
        finally:
            print("Zamykanie producenta...")
            producer_instance.close()
            print("Producent zamknięty.")