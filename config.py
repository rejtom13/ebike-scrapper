import os
from dotenv import load_dotenv

# Załaduj zmienne środowiskowe z pliku .env
load_dotenv()

# Konfiguracja bazy danych
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", 5432),
    'database': os.getenv("DB_NAME", "olx_data"),
    'user': os.getenv("DB_USER", "postgres"),
    'password': os.getenv("DB_PASSWORD")
}

# Sprawdzenie, czy wszystkie zmienne DB zostały załadowane
if not DB_CONFIG['password']:
    print("BŁĄD: Zmienne środowiskowe bazy danych (np. DB_PASSWORD) nie są ustawione.")
    print("Upewnij się, że masz plik .env z poprawnymi danymi.")
    # Można tu rzucić wyjątek, aby przerwać działanie skryptu
    # raise ValueError("Brak konfiguracji bazy danych w pliku .env")


# Stałe
HEADERS = {
    'accept': 'application/json',
    'accept-language': 'pl',
    'content-type': 'application/json',
    'origin': 'https://www.olx.pl',
    'referer': 'https://www.olx.pl/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-client': 'DESKTOP'
}

API_URL = 'https://www.olx.pl/apigateway/graphql'

# Zapytanie GraphQL (przeniesione z klasy dla czytelności)
GRAPHQL_QUERY = """
query ListingSearchQuery($searchParameters: [SearchParameter!] = {key: "", value: ""}) {
  clientCompatibleListings(searchParameters: $searchParameters) {
    __typename
    ... on ListingSuccess {
      data {
        id
        title
        url
        description
        created_time
        last_refresh_time
        status
        offer_type
        business
        protect_phone
        location {
          city { id name }
          district { id name }
          region { id name }
        }
        map {
          lat
          lon
          radius
          zoom
          show_detailed
        }
        valid_to_time
        category {
          id
          type
        }
        contact {
          chat
          name
          negotiation
          phone
          courier
        }
        photos {
          link
        }
        promotion {
          highlighted
          top_ad
          urgent
          premium_ad_page
          b2c_ad_page
          options
        }
        user {
          id
          uuid
          name
          seller_type
          created
          is_online
          last_seen
        }
        business
        params {
          key
          name
          type
          value {
            __typename
            ... on PriceParam {
              value
              currency
              negotiable
              label
            }
            ... on GenericParam {
              key
              label
            }
          }
        }
      }
      metadata {
        total_elements
        visible_total_count
      }
      links {
        next { href }
      }
    }
    ... on ListingError {
      error {
        code
        detail
      }
    }
  }
}
"""