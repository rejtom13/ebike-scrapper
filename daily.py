import config
from database import Database
from scraper import OLXGraphQLScraper
import time

# ========== CODZIENNE URUCHOMIENIE (PEŁNE SKANOWANIE) ==========

if __name__ == "__main__":

    if not config.DB_CONFIG['password']:
        print("BŁĄD KRYTYCZNY: Brak hasła do bazy danych w pliku .env")
        print("Zatrzymałem działanie skryptu.")
    else:
        try:
            start_time = time.time()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Uruchamiam codzienne PEŁNE pobieranie...")

            print("Łączenie z bazą danych...")
            db = Database(db_config=config.DB_CONFIG)

            # === KROK 1: Deaktywacja wszystkich ogłoszeń ===
            # To jest kluczowy element! Ustawiamy is_active=FALSE dla wszystkich.
            db.deactivate_all_listings()

            # Pobieranie statystyk PRZED uruchomieniem
            print("\n--- Statystyki PRZED (po deaktywacji) ---")
            db.get_stats()  # Pokaże 0 aktywnych (jeśli dodasz taki filtr do stats)

            scraper = OLXGraphQLScraper(database=db)

            CATEGORY_ELECTRIC_BIKES = 767

            # Ustaw 'None', aby pobrać wszystkie (nowe i używane)
            STATE_FILTER = None

            # Ustaw pełen zakres, który Cię interesuje.
            PRICE_FROM_FILTER = 1000.0
            PRICE_TO_FILTER = 50000.0

            # === KROK 2: Uruchomienie pełnego skanowania ===
            # Używamy scrape_recursive, aby pobrać WSZYSTKIE ogłoszenia
            # Funkcja save_to_database automatycznie ustawi im is_active=TRUE
            listings = scraper.scrape_recursive(
                query='rowery elektryczne',
                target_results=50000,  # Ustaw duży limit, aby pobrać wszystko
                batch_size=40,
                category_id=CATEGORY_ELECTRIC_BIKES,
                state=STATE_FILTER,
                initial_price_from=PRICE_FROM_FILTER,
                initial_price_to=PRICE_TO_FILTER
            )

            # Pobieranie statystyk PO uruchomieniu
            print("\n--- Statystyki PO ---")
            db.get_stats()

            end_time = time.time()
            print(f"\nCałkowity czas operacji: {end_time - start_time:.2f} sek.")
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Zakończono.")

        except Exception as e:
            print(f"\nNapotkano nieoczekiwany błąd główny: {e}")