import requests
import time
import json
from datetime import datetime
from collections import deque

# Importujemy stałe i konfigurację z pliku config.py
import config


class OLXGraphQLScraper:
    OLX_LIMIT = 999

    def __init__(self, database):
        self.api_url = config.API_URL
        self.headers = config.HEADERS
        self.graphql_query = config.GRAPHQL_QUERY
        self.db = database

    def search(self, query, offset=0, limit=40, sort_by="created_at:desc", price_from=None, price_to=None,
               category_id=None, state=None):
        """Wysyła zapytanie GraphQL do API OLX."""
        search_params = [
            {"key": "offset", "value": str(offset)},
            {"key": "limit", "value": str(limit)},
            {"key": "query", "value": query},
            {"key": "sort_by", "value": sort_by},
            {"key": "filter_refiners", "value": "spell_checker"}
        ]

        if category_id is not None:
            search_params.append({"key": "category_id", "value": str(category_id)})
        if price_from is not None:
            search_params.append({"key": "filter_float_price:from", "value": f"{price_from:.2f}"})
        if price_to is not None:
            search_params.append({"key": "filter_float_price:to", "value": f"{price_to:.2f}"})

        if state is not None and state in ["new", "used"]:
            search_params.append({"key": "filter_enum_state[0]", "value": str(state)})

        payload = {
            "query": self.graphql_query,
            "variables": {"searchParameters": search_params}
        }

        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=self.headers,
                timeout=20
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"✗ Błąd HTTP: {e.response.status_code} {e.response.reason}")
        except requests.exceptions.RequestException as e:
            print(f"✗ Błąd połączenia z API: {e}")
        return None

    def _get_total_count(self, query, category_id, price_from, price_to, state=None):
        """Pobiera łączną liczbę wyników dla danego zapytania (limit=1)."""
        if price_from is not None and price_to is not None:
            print(f"   [SPRAWDZAM] Zakres cen: {price_from:.2f} - {price_to:.2f}...")
        else:
            print(f"   [SPRAWDZAM] Zakres cen: Całość...")

        response = self.search(
            query,
            offset=0,
            limit=1,
            sort_by="created_at:desc",
            price_from=price_from,
            price_to=price_to,
            category_id=category_id,
            state=state
        )

        if not response:
            return None

        listings_data = response.get('data', {}).get('clientCompatibleListings', {})

        if listings_data.get('__typename') == 'ListingError':
            error = listings_data.get('error', {})
            print(f"   ✗ Błąd API przy sprawdzaniu: {error.get('detail')}")
            return None

        if listings_data.get('__typename') == 'ListingSuccess':
            metadata = listings_data.get('metadata', {})
            total = metadata.get('total_elements', 0)
            visible_total = metadata.get('visible_total_count', 0)
            count = max(total, visible_total)

            print(f"   [INFO] Znaleziono {count} ogłoszeń w tym zakresie.")
            return count

        return None

    def _get_bound_price(self, query, category_id, sort_by, state=None):
        """
        Pobiera cenę graniczną (min/max) dla danego sortowania.
        Używa limit=40 i szuka PIERWSZEGO NIEPROMOWANEGO ogłoszenia z ceną.
        """
        print(f"   [INFO] Pobieranie ceny granicznej (sortowanie: {sort_by})...")
        print(f"   [INFO] Szukanie pierwszego *niepromowanego* ogłoszenia z ceną...")
        response = self.search(
            query,
            offset=0,
            limit=40,
            sort_by=sort_by,
            category_id=category_id,
            state=state
        )

        if not response:
            return None

        listings_data = response.get('data', {}).get('clientCompatibleListings', {})

        if listings_data.get('__typename') == 'ListingSuccess':
            batch = listings_data.get('data', [])

            non_promoted_price = None
            first_price_any = None

            for listing in batch:
                parsed = self.parse_listing(listing)
                price = parsed.get('price_value')
                is_promoted = parsed.get('promoted', False)

                if price is not None:
                    price_float = float(price)

                    if first_price_any is None:
                        first_price_any = price_float

                    if not is_promoted:
                        non_promoted_price = price_float
                        print(f"   [INFO] Znaleziono niepromowaną cenę graniczną: {non_promoted_price:.2f}")
                        return non_promoted_price

            if non_promoted_price is None:
                if first_price_any is not None:
                    print(f"   [OSTRZEŻENIE] Nie znaleziono niepromowanej ceny na 1. stronie.")
                    print(f"   [OSTRZEŻENIE] Używam pierwszej znalezionej ceny (promowanej): {first_price_any:.2f}")
                    return first_price_any
                else:
                    print(
                        f"   ✗ Nie udało się pobrać ceny granicznej dla {sort_by} (brak ogłoszeń z ceną w pierwszej partii)")
                    return None

        print(f"   ✗ Nie udało się pobrać ceny granicznej dla {sort_by} (błąd API lub brak 'ListingSuccess')")
        return None

    # ... (funkcje extract_price, parse_timestamp, parse_listing pozostają bez zmian) ...

    def extract_price(self, params):
        """Wyciąga dane o cenie z listy parametrów."""
        for param in params:
            if param.get('key') == 'price':
                value = param.get('value', {})
                if value.get('__typename') == 'PriceParam':
                    return {
                        'value': value.get('value'),
                        'currency': value.get('currency'),
                        'label': value.get('label'),
                        'negotiable': value.get('negotiable', False)
                    }
        return None

    def parse_timestamp(self, timestamp_str):
        """Konwertuje timestamp ISO z OLX na obiekt datetime."""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            print(f"Nie udało się sparsować daty: {timestamp_str}")
            return None

    def parse_listing(self, listing):
        """Przetwarza surowy słownik JSON ogłoszenia na format do bazy danych."""
        location = listing.get('location') or {}
        map_data = listing.get('map') or {}
        user = listing.get('user') or {}
        contact = listing.get('contact') or {}
        promotion = listing.get('promotion') or {}
        price_info = self.extract_price(listing.get('params', []))

        # Zbieranie parametrów do JSONB
        params_list = []
        for param in listing.get('params', []):
            param_value = param.get('value') or {}
            if param_value.get('__typename') == 'GenericParam':
                params_list.append({
                    'name': param.get('name'),
                    'key': param.get('key'),
                    'value': param_value.get('label')
                })

        photos = listing.get('photos', [])
        photo_urls = [photo.get('link', '').replace('{width}', '1200').replace('{height}', '900') for photo in photos]

        city = location.get('city') or {}
        region = location.get('region') or {}
        district = location.get('district') or {}
        category = listing.get('category') or {}

        return {
            'olx_id': listing.get('id'),
            'title': listing.get('title'),
            'url': listing.get('url'),
            'description': listing.get('description', ''),
            'created_time': self.parse_timestamp(listing.get('created_time')),
            'refreshed_time': self.parse_timestamp(listing.get('last_refresh_time')),
            'valid_to_time': self.parse_timestamp(listing.get('valid_to_time')),
            'offer_type': listing.get('offer_type'),
            'business': listing.get('business', False),
            'price_value': price_info.get('value') if price_info else None,
            'price_label': price_info.get('label') if price_info else 'Brak ceny',
            'currency': price_info.get('currency') if price_info else None,
            'negotiable': price_info.get('negotiable') if price_info else False,
            'location_city': city.get('name'),
            'location_region': region.get('name'),
            'location_district': district.get('name') if district else None,
            'latitude': map_data.get('lat'),
            'longitude': map_data.get('lon'),
            'map_radius': map_data.get('radius'),
            'map_zoom': map_data.get('zoom'),
            'user_id': user.get('uuid'),
            'user_name': user.get('name'),
            'user_type': user.get('seller_type'),
            'user_created': self.parse_timestamp(user.get('created')),
            'user_last_seen': self.parse_timestamp(user.get('last_seen')),
            'user_is_online': user.get('is_online', False),
            'category_id': category.get('id'),
            'promoted': promotion.get('top_ad', False),  # <-- To jest flaga ogłoszenia 'TOP'
            'highlighted': promotion.get('highlighted', False),
            'urgent': promotion.get('urgent', False),
            'premium_ad': promotion.get('premium_ad_page', False),
            'promotion_options': promotion.get('options', []),
            'photos_count': len(photos),
            'photos_urls': photo_urls,
            'phone_protected': listing.get('protect_phone', False),
            'chat_available': contact.get('chat', False),
            'courier_available': contact.get('courier', False),
            'params': json.dumps(params_list, ensure_ascii=False) if params_list else None,
            'scraped_at': datetime.now()
        }

    def _scrape_batch(self, query, sort_by="created_at:desc", max_results=1000, batch_size=40, price_from=None,
                      price_to=None, category_id=None, state=None):
        """
        Pobiera jedną partię ogłoszeń (do 1000) dla określonych filtrów.
        """
        effective_max = min(max_results, self.OLX_LIMIT)
        listings = []
        offset = 0
        total_available_in_range = 0

        while len(listings) < effective_max:
            if offset >= 1000:
                print(f"   ⚠️  Osiągnięto limit offsetu (1000). Zatrzymuję pobieranie tej partii.")
                break

            print(f"   📥 Pobieranie: Offset={offset}, Limit={batch_size}")

            time.sleep(0.5)

            response = self.search(
                query,
                offset=offset,
                limit=batch_size,
                sort_by=sort_by,
                price_from=price_from,
                price_to=price_to,
                category_id=category_id,
                state=state
            )

            if not response:
                break

            listings_data = response.get('data', {}).get('clientCompatibleListings', {})

            if listings_data.get('__typename') != 'ListingSuccess':
                print("   ✗ Błąd API lub brak wyników (ListingSuccess != true).")
                break

            batch = listings_data.get('data', [])
            if not batch:
                print(f"   ✓ Koniec wyników w tym zakresie.")
                break

            if offset == 0:
                metadata = listings_data.get('metadata', {})
                total_available_in_range = metadata.get('total_elements', 0)
                print(f"      (Info: Dostępnych w tym zakresie: {total_available_in_range})")

            parsed = [self.parse_listing(listing) for listing in batch]
            listings.extend(parsed)

            if len(batch) < batch_size or len(listings) >= effective_max:
                break

            offset += batch_size

        print(f"   ✅ Zebrano {len(listings)} ogłoszeń z tego zakresu.")
        return listings

    def _print_summary(self, total_fetched, total_saved):
        """Wyświetla podsumowanie całego procesu."""
        print(f"\n{'=' * 60}")
        print("🎉 ZAKOŃCZONO SCRAPING")
        print(f"{'=' * 60}")
        print(f"📦 Łącznie pobrano: {total_fetched} unikalnych ogłoszeń")
        print(f"💾 Zapisano/Zaktualizowano w bazie: {total_saved} ogłoszeń")

    def scrape_latest(self, query, max_results=1000, batch_size=40, category_id=None, state=None,
                      price_from=None, price_to=None):
        """
        Pobiera najnowsze ogłoszenia (posortowane po dacie) do limitu 'max_results'.
        Używa _scrape_batch bez rekurencji. Idealne do codziennych aktualizacji.
        """
        print(f"\n🚀 Rozpoczynam szybki scraping (najnowsze {max_results}) dla: '{query}'")
        if category_id:
            print(f"📁 Kategoria: {category_id}")
        if state:
            print(f"🔄 Stan: {state}")
        if price_from or price_to:
            print(f"💰 Zakres cen: {price_from} - {price_to}")

        # Używamy _scrape_batch, aby pobrać tylko pierwszą partię wyników
        # posortowaną po dacie (domyślny sort_by to 'created_at:desc')
        listings = self._scrape_batch(
            query,
            sort_by="created_at:desc",  # Upewniamy się, że sortuje po dacie
            max_results=min(max_results, self.OLX_LIMIT),  # Nie przekraczamy limitu OLX (999)
            batch_size=batch_size,
            price_from=price_from,
            price_to=price_to,
            category_id=category_id,
            state=state
        )

        if not listings:
            print("   ✓ Nie znaleziono żadnych ogłoszeń pasujących do kryteriów.")
            self._print_summary(0, 0)
            return 0

        print(f"\n   💾 Zapisywanie {len(listings)} pobranych ogłoszeń do bazy danych...")
        saved_count = self.db.save_to_database(listings)

        self._print_summary(len(listings), saved_count)
        return saved_count

    def scrape_recursive(self, query, target_results=5000, batch_size=40, category_id=None, state=None,
                         initial_price_from=1.0, initial_price_to=None):  # <-- NOWE PARAMETRY
        """
        Główna funkcja scrapująca, używająca rekurencyjnego podziału cenowego.
        """
        print(f"\n🚀 Rozpoczynam scraping dla: '{query}'")
        if category_id:
            print(f"📁 Kategoria: {category_id}")

        if state:
            print(f"🔄 Stan: {state}")

        print(f"🎯 Cel: {target_results} ogłoszeń")
        print(f"💡 Strategia: Rekurencyjny podział cenowy (limit OLX: {self.OLX_LIMIT})\n")

        all_listings = {}
        total_saved_count = 0

        task_queue = deque()

        # ==================================================================
        # *** POPRAWKA: Używamy przekazanych zakresów cenowych ***
        # Sprawdzamy liczbę ogłoszeń w TYM KONKRETNYM ZAKRESIE CENOWYM
        # ==================================================================
        print(f"   [INFO] Używam początkowego zakresu cen: {initial_price_from} - {initial_price_to}")
        initial_total = self._get_total_count(query, category_id, initial_price_from, initial_price_to, state)

        if initial_total is None:
            print("✗ Nie udało się pobrać wstępnych danych. Przerywam.")
            return []

        if 0 < initial_total <= self.OLX_LIMIT:
            print(f"✓ Łączna liczba ogłoszeń ({initial_total}) jest mniejsza lub równa limitowi.")
            print("Pobieram wszystko w jednej partii...")
            listings_batch = self._scrape_batch(
                query,
                max_results=min(initial_total, target_results),
                batch_size=batch_size,
                category_id=category_id,
                state=state,
                price_from=initial_price_from,  # <-- Filtrujemy tylko w tym zakresie
                price_to=initial_price_to
            )
            for listing in listings_batch:
                all_listings[listing['olx_id']] = listing

            saved = self.db.save_to_database(listings_batch)
            total_saved_count += saved
            print(f"   💾 Zapisano/Zaktualizowano: {saved} ogłoszeń")

        elif initial_total > self.OLX_LIMIT:
            print(f"⚠️ Łączna liczba ogłoszeń ({initial_total}) przekracza limit {self.OLX_LIMIT}.")
            print("Rozpoczynam dzielenie na zakresy cenowe...")

            # ==================================================================
            # *** POPRAWKA: Ustawienie zakresu cenowego (min/max) ***
            # ==================================================================
            min_price = initial_price_from
            print(f"   [INFO] Dolny zakres ceny ustawiony na: {min_price:.2f} PLN")

            if initial_price_to is None:
                # Jeśli użytkownik nie podał górnego zakresu, szukamy go dynamicznie
                print("   [INFO] Górny zakres (initial_price_to) nieustawiony, szukam dynamicznie...")
                max_price = self._get_bound_price(query, category_id, "filter_float_price:desc", state)
                if max_price is None:
                    print("✗ Nie udało się ustalić ceny maksymalnej. Przerywam.")
                    return []
            else:
                # Używamy górnego zakresu podanego przez użytkownika
                max_price = initial_price_to
                print(f"   [INFO] Górny zakres ceny ustawiony na sztywno: {max_price:.2f} PLN")
            # ==================================================================

            if max_price < min_price:
                print(
                    f"   [OSTRZEŻENIE] Znaleziona/ustawiona cena maksymalna ({max_price:.2f}) jest mniejsza niż startowa ({min_price:.2f}). Używam {min_price:.2f} - {min_price:.2f}.")
                max_price = min_price

            # Dodajemy do kolejki PIERWSZE zadanie z pełnym zakresem
            task_queue.append((min_price, max_price))
            print(f"   [INFO] Ustalono pełny zakres do podziału: {min_price:.2f} - {max_price:.2f} PLN")

            # 3. Pętla przetwarzania kolejki zadań
            while task_queue and len(all_listings) < target_results:
                p_from, p_to = task_queue.popleft()

                if p_from is not None and p_to is not None and p_from > p_to:
                    print(f"   [OSTRZEŻENIE] Pominąłem nieprawidłowy zakres: {p_from:.2f} > {p_to:.2f}")
                    continue

                print(f"\nProcessing range: {p_from:.2f} - {p_to:.2f}")

                # Sprawdzamy, ile jest ogłoszeń w *tym konkretnym pod-zakresie*
                current_total = self._get_total_count(query, category_id, p_from, p_to, state)

                if current_total is None or current_total == 0:
                    print("   [INFO] Brak wyników w tym zakresie. Pomijam.")
                    continue

                if 0 < current_total <= self.OLX_LIMIT:
                    # Ten zakres jest wystarczająco mały, aby go pobrać!
                    print(f"   [OK] Zakres {p_from:.2f}-{p_to:.2f} ma {current_total} ogłoszeń. Pobieram...")

                    remaining_needed = target_results - len(all_listings)

                    listings_batch = self._scrape_batch(
                        query,
                        max_results=min(remaining_needed, self.OLX_LIMIT, current_total),
                        batch_size=batch_size,
                        price_from=p_from,
                        price_to=p_to,
                        category_id=category_id,
                        state=state
                    )

                    new_listings_in_batch = []
                    for listing in listings_batch:
                        if listing['olx_id'] not in all_listings:
                            all_listings[listing['olx_id']] = listing
                            new_listings_in_batch.append(listing)

                    if new_listings_in_batch:
                        saved = self.db.save_to_database(new_listings_in_batch)
                        total_saved_count += saved
                        print(f"   💾 Dodano {len(new_listings_in_batch)} nowych ogłoszeń (Zapisano/Zakt: {saved})")
                    else:
                        print("   ✓ Brak nowych ogłoszeń w tej partii.")

                elif current_total > self.OLX_LIMIT:
                    # Ten zakres jest nadal za duży. Podziel go.

                    can_split = True
                    if p_from is not None and p_to is not None:
                        if (p_to - p_from) < 0.01:
                            can_split = False

                    if can_split:
                        p_mid = (p_from + p_to) / 2.0
                        print(f"   [SPLIT] Zakres {p_from:.2f}-{p_to:.2f} jest za duży ({current_total}).")
                        print(f"   Dzielę na: {p_from:.2f}-{p_mid:.2f} i {p_mid:.2f}-{p_to:.2f}")

                        task_queue.appendleft((p_mid, p_to))
                        task_queue.appendleft((p_from, p_mid))

                    else:
                        print(
                            f"   [OSTRZEŻENIE] Nie można dalej podzielić zakresu {p_from:.2f}-{p_to:.2f} (total: {current_total}).")
                        print(f"   Pobieram pierwsze {self.OLX_LIMIT} ogłoszeń z tego zakresu (limit OLX).")

                        remaining_needed = target_results - len(all_listings)
                        listings_batch = self._scrape_batch(
                            query,
                            max_results=min(remaining_needed, self.OLX_LIMIT),
                            batch_size=batch_size,
                            price_from=p_from,
                            price_to=p_to,
                            category_id=category_id,
                            state=state
                        )

                        new_listings_in_batch = []
                        for listing in listings_batch:
                            if listing['olx_id'] not in all_listings:
                                all_listings[listing['olx_id']] = listing
                                new_listings_in_batch.append(listing)

                        if new_listings_in_batch:
                            saved = self.db.save_to_database(new_listings_in_batch)
                            total_saved_count += saved
                            print(f"   💾 Dodano {len(new_listings_in_batch)} nowych ogłoszeń (Zapisano/Zakt: {saved})")
                        else:
                            print("   ✓ Brak nowych ogłoszeń w tej partii.")

        # 4. Koniec
        final_listings_list = list(all_listings.values())
        self._print_summary(len(final_listings_list), total_saved_count)
        return final_listings_list