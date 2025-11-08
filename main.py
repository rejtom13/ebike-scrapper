import config
from database import Database
from scraper import OLXGraphQLScraper

# ========== GŁÓWNY PUNKT URUCHOMIENIA ==========

if __name__ == "__main__":

    if not config.DB_CONFIG['password']:
        print("BŁĄD KRYTYCZNY: Brak hasła do bazy danych w pliku .env")
        print("Zatrzymałem działanie skryptu.")
    else:
        try:
            print("Łączenie z bazą danych...")
            db = Database(db_config=config.DB_CONFIG)

            scraper = OLXGraphQLScraper(database=db)

            CATEGORY_ELECTRIC_BIKES = 767

            # *** NOWA OPCJA: 'state' ***
            # Ustaw 'None', aby pobrać wszystkie (nowe i używane)
            # Ustaw "used", aby pobrać tylko używane
            # Ustaw "new", aby pobrać tylko nowe
            STATE_FILTER = None

            # ==========================================================
            # *** NOWE FILTRY CENOWE (min / max) ***
            # Ustaw pełen zakres, który Cię interesuje.
            # ==========================================================
            PRICE_FROM_FILTER = 1000.0  # Dolny zakres (zgodnie z prośbą)
            PRICE_TO_FILTER = 50000.0  # Górny zakres (np. 50 000 zł)

            # Jeśli chcesz dynamicznie szukać ceny maksymalnej, ustaw:
            # PRICE_TO_FILTER = None
            # ==========================================================

            listings = scraper.scrape_recursive(
                query='rowery elektryczne',
                target_results=50000,
                batch_size=40,
                category_id=CATEGORY_ELECTRIC_BIKES,
                state=STATE_FILTER,
                initial_price_from=PRICE_FROM_FILTER,  # <-- Przekazanie dolnego zakresu
                initial_price_to=PRICE_TO_FILTER  # <-- Przekazanie górnego zakresu
            )

            db.get_stats()

            print("\n📋 Przykładowe pobrane ogłoszenia:")
            for i, listing in enumerate(listings[:3], 1):
                print(f"\n{i}. {listing['title']}")
                print(f"   💰 Cena: {listing['price_label']}")
                print(f"   📍 Lokalizacja: {listing['location_city']}")
                print(f"   🔗 URL: {listing['url']}")

        except Exception as e:
            print(f"\nNapotkano nieoczekiwany błąd główny: {e}")