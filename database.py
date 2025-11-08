import psycopg2
from psycopg2.extras import execute_values


class Database:
    """Klasa do zarządzania połączeniem i operacjami na bazie danych PostgreSQL."""

    def __init__(self, db_config):
        """
        Inicjalizuje obiekt bazy danych i od razu tworzy tabelę, jeśli nie istnieje.

        Args:
            db_config (dict): Słownik konfiguracyjny dla psycopg2.
        """
        self.db_config = db_config
        self.setup_database()

    def get_connection(self):
        """Tworzy i zwraca nowe połączenie PostgreSQL."""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except psycopg2.OperationalError as e:
            print(f"✗ BŁĄD: Nie można połączyć się z bazą danych: {e}")
            print("Sprawdź konfigurację w pliku .env oraz czy baza PostgreSQL działa.")
            return None

    def setup_database(self):
        """Tworzy tabelę 'listings' wraz z indeksami, jeśli nie istnieje,
           oraz dodaje kolumnę 'is_active', jeśli jej brakuje."""
        conn = self.get_connection()
        if conn is None:
            return

        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'listings'
                )
            """)
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                print("Tabela 'listings' nie istnieje. Tworzenie...")
                cursor.execute('''
                    CREATE TABLE listings (
                        id SERIAL PRIMARY KEY,
                        olx_id VARCHAR(100) UNIQUE NOT NULL,
                        title TEXT,
                        price_label VARCHAR(100),
                        price_value DECIMAL(10, 2),
                        currency VARCHAR(10),
                        negotiable BOOLEAN DEFAULT FALSE,
                        location_city VARCHAR(100),
                        location_region VARCHAR(100),
                        location_district VARCHAR(100),
                        latitude DECIMAL(10, 7),
                        longitude DECIMAL(10, 7),
                        map_radius INTEGER,
                        map_zoom INTEGER,
                        created_time TIMESTAMP,
                        refreshed_time TIMESTAMP,
                        valid_to_time TIMESTAMP,
                        url TEXT,
                        description TEXT,
                        offer_type VARCHAR(50),
                        business BOOLEAN DEFAULT FALSE,
                        user_id VARCHAR(100),
                        user_name VARCHAR(200),
                        user_type VARCHAR(50),
                        user_created TIMESTAMP,
                        user_last_seen TIMESTAMP,
                        user_is_online BOOLEAN DEFAULT FALSE,
                        category_id VARCHAR(50),
                        promoted BOOLEAN DEFAULT FALSE,
                        highlighted BOOLEAN DEFAULT FALSE,
                        urgent BOOLEAN DEFAULT FALSE,
                        premium_ad BOOLEAN DEFAULT FALSE,
                        promotion_options TEXT[],
                        photos_count INTEGER DEFAULT 0,
                        photos_urls TEXT[],
                        params JSONB,
                        phone_protected BOOLEAN DEFAULT FALSE,
                        chat_available BOOLEAN DEFAULT FALSE,
                        courier_available BOOLEAN DEFAULT FALSE,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE  -- <-- NOWA KOLUMNA
                    )
                ''')

                # Tworzenie indeksów
                print("Tworzenie indeksów...")
                cursor.execute('CREATE INDEX idx_olx_id ON listings(olx_id)')
                cursor.execute('CREATE INDEX idx_price_value ON listings(price_value)')
                cursor.execute('CREATE INDEX idx_location_city ON listings(location_city)')
                cursor.execute('CREATE INDEX idx_created_time ON listings(created_time)')
                cursor.execute('CREATE INDEX idx_location_coords ON listings(latitude, longitude)')
                cursor.execute('CREATE INDEX idx_business ON listings(business)')
                cursor.execute('CREATE INDEX idx_params ON listings USING gin(params)')
                cursor.execute('CREATE INDEX idx_is_active ON listings(is_active)')  # <-- NOWY INDEKS

                conn.commit()
                print("✓ Tabela 'listings' oraz indeksy zostały utworzone.")
            else:
                print("✓ Tabela 'listings' już istnieje. Sprawdzam kolumnę 'is_active'...")
                # Sprawdź, czy kolumna 'is_active' już istnieje
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns 
                        WHERE table_name = 'listings' AND column_name = 'is_active'
                    )
                """)
                column_exists = cursor.fetchone()[0]

                if not column_exists:
                    print("   [DB] Kolumna 'is_active' nie istnieje. Dodawanie...")
                    cursor.execute("ALTER TABLE listings ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
                    cursor.execute("CREATE INDEX idx_is_active ON listings(is_active)")  # <-- NOWY INDEKS
                    conn.commit()
                    print("   [DB] ✓ Kolumna 'is_active' i indeks dodane.")
                else:
                    print("   [DB] ✓ Kolumna 'is_active' już istnieje.")


        except Exception as e:
            print(f"✗ Błąd podczas tworzenia/aktualizacji tabeli: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def deactivate_all_listings(self):
        """Ustawia flagę 'is_active = FALSE' dla wszystkich aktywnych ogłoszeń."""
        print("\n[DB] Deaktywowanie wszystkich ogłoszeń przed skanowaniem...")
        conn = self.get_connection()
        if conn is None:
            return 0

        cursor = conn.cursor()
        try:
            # Deaktywujemy tylko te, które są obecnie aktywne
            cursor.execute("UPDATE listings SET is_active = FALSE WHERE is_active = TRUE")
            deactivated_count = cursor.rowcount
            conn.commit()
            print(f"[DB] ✓ Oznaczono {deactivated_count} ogłoszeń jako nieaktywne.")
            return deactivated_count
        except Exception as e:
            print(f"✗ Błąd podczas deaktywacji ogłoszeń: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            conn.close()

    def save_to_database(self, listings_data):
        """
        Zapisuje listę ogłoszeń do bazy danych PostgreSQL (INSERT ... ON CONFLICT).
        Wszystkie zapisywane/aktualizowane ogłoszenia są oznaczane jako 'is_active = TRUE'.

        Args:
            listings_data (list): Lista słowników z danymi ogłoszeń.

        Returns:
            int: Liczba zapisanych/zaktualizowanych wierszy.
        """
        if not listings_data:
            return 0

        # Usuwanie duplikatów w ramach tej samej paczki danych
        seen_ids = set()
        unique_listings = []
        for listing in listings_data:
            if listing['olx_id'] not in seen_ids:
                seen_ids.add(listing['olx_id'])
                unique_listings.append(listing)

        if len(unique_listings) < len(listings_data):
            duplicates = len(listings_data) - len(unique_listings)
            print(f"   ⚠️  Usunięto {duplicates} duplikatów wewnątrz tej paczki danych.")

        if not unique_listings:
            return 0

        conn = self.get_connection()
        if conn is None:
            return 0

        cursor = conn.cursor()

        # Zapytanie z ON CONFLICT DO UPDATE
        insert_query = """
            INSERT INTO listings (
                olx_id, title, price_label, price_value, currency, negotiable,
                location_city, location_region, location_district, latitude, longitude,
                map_radius, map_zoom, created_time, refreshed_time, valid_to_time,
                url, description, offer_type, business, user_id, user_name, user_type,
                user_created, user_last_seen, user_is_online, category_id,
                promoted, highlighted, urgent, premium_ad, promotion_options,
                photos_count, photos_urls, params, phone_protected, chat_available,
                courier_available, scraped_at, is_active
            ) VALUES %s
            ON CONFLICT (olx_id) DO UPDATE SET
                price_value = EXCLUDED.price_value,
                price_label = EXCLUDED.price_label,
                refreshed_time = EXCLUDED.refreshed_time,
                promoted = EXCLUDED.promoted,
                highlighted = EXCLUDED.highlighted,
                urgent = EXCLUDED.urgent,
                photos_count = EXCLUDED.photos_count,
                photos_urls = EXCLUDED.photos_urls,
                params = EXCLUDED.params,
                description = EXCLUDED.description,
                title = EXCLUDED.title,
                updated_at = CURRENT_TIMESTAMP,
                is_active = TRUE
        """

        # Przygotowanie danych do execute_values
        values = [
            (
                listing['olx_id'], listing['title'], listing['price_label'],
                listing['price_value'], listing['currency'], listing['negotiable'],
                listing['location_city'], listing['location_region'],
                listing['location_district'], listing['latitude'], listing['longitude'],
                listing['map_radius'], listing['map_zoom'], listing['created_time'],
                listing['refreshed_time'], listing['valid_to_time'], listing['url'],
                listing['description'], listing['offer_type'], listing['business'],
                listing['user_id'], listing['user_name'], listing['user_type'],
                listing['user_created'], listing['user_last_seen'], listing['user_is_online'],
                listing['category_id'], listing['promoted'], listing['highlighted'],
                listing['urgent'], listing['premium_ad'], listing['promotion_options'],
                listing['photos_count'], listing['photos_urls'], listing['params'],
                listing['phone_protected'], listing['chat_available'],
                listing['courier_available'], listing['scraped_at'],
                True  # <-- Ustawiamy 'is_active = TRUE' dla wstawianych/aktualizowanych
            ) for listing in unique_listings
        ]

        try:
            execute_values(cursor, insert_query, values)
            saved = cursor.rowcount
            conn.commit()
            return saved
        except Exception as e:
            print(f"✗ Błąd podczas zapisu do bazy: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            conn.close()

    def get_stats(self):
        """Pobiera i wyświetla podstawowe statystyki z bazy danych."""
        conn = self.get_connection()
        if conn is None:
            print("✗ Nie można pobrać statystyk, brak połączenia z bazą.")
            return

        cursor = conn.cursor()

        try:
            cursor.execute("SELECT COUNT(*) FROM listings")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM listings WHERE is_active = TRUE")
            total_active = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM listings WHERE is_active = FALSE")
            total_inactive = cursor.fetchone()[0]

            cursor.execute(
                "SELECT AVG(price_value) FROM listings WHERE price_value IS NOT NULL AND currency = 'PLN' AND is_active = TRUE")
            avg_price_result = cursor.fetchone()[0]
            avg_price = float(avg_price_result) if avg_price_result else None

            cursor.execute("SELECT COUNT(*) FROM listings WHERE promoted = TRUE AND is_active = TRUE")
            promoted = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(created_time), MAX(created_time) FROM listings WHERE is_active = TRUE")
            min_date, max_date = cursor.fetchone()

            print(f"\n{'=' * 60}")
            print("📊 STATYSTYKI BAZY DANYCH")
            print(f"{'=' * 60}")
            print(f"   Ogłoszenia łącznie (w bazie): {total}")
            print(f"   Ogłoszenia AKTYWNE: {total_active}")
            print(f"   Ogłoszenia NIEAKTYWNE: {total_inactive}")
            if avg_price:
                print(f"   Średnia cena (Aktywne, PLN): {avg_price:.2f} PLN")
            else:
                print("   Średnia cena (Aktywne, PLN): Brak danych")
            print(f"   Promowane (Aktywne): {promoted}")
            if min_date:
                print(f"   Zakres dat (Aktywne): od {min_date.strftime('%Y-%m-%d')} do {max_date.strftime('%Y-%m-%d')}")

        except Exception as e:
            print(f"✗ Błąd podczas pobierania statystyk: {e}")
        finally:
            cursor.close()
            conn.close()