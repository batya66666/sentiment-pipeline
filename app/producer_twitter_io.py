import json
import time
import requests
from kafka import KafkaProducer
import datetime

# --- ТВОЙ КЛЮЧ ---
# Вставь сюда ключ, который ты скопировал с сайта (начинается на ...)
API_KEY = "new1_076eb80c2ba24d08b8ba033a4d94cc6e"

# --- НАСТРОЙКИ ---
SEARCH_QUERY = "(bitcoin OR ai OR python) lang:en"
KAFKA_TOPIC = "social_data"
KAFKA_SERVER = "kafka:9092"

# ПРАВИЛЬНЫЙ URL для поиска
URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... ({e})")
            time.sleep(5)

def main():
    producer = get_producer()
    
    headers = {
        "x-api-key": API_KEY
    }
    
    print(f"🚀 Starting TwitterAPI.io Stream for: {SEARCH_QUERY}...")

    while True:
        try:
            params = {
                "query": SEARCH_QUERY,
                "limit": 20 
            }
            
            response = requests.get(URL, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                tweets = data.get("tweets", [])
                
                if not tweets:
                    print("⚠️ No tweets found in response, waiting...")
                
                for tweet in tweets:
                    text_content = tweet.get("text") or tweet.get("full_text") or ""
                    
                    if not text_content:
                        continue

                    msg = {
                        "source": "Twitter (Real)",
                        "text": text_content,
                        "created_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    producer.send(KAFKA_TOPIC, msg)
                    print(f"[Twitter] Sent: {msg['text'][:50]}...")
            
            elif response.status_code == 401 or response.status_code == 403:
                print(f"❌ Ошибка доступа (Проверь ключ или баланс): {response.text}")
                break # Останавливаемся, чтобы не спамить ошибками
            elif response.status_code == 404:
                print(f"❌ Неверный URL API: {URL}")
                break
            else:
                print(f"⚠️ API Error {response.status_code}: {response.text}")

            time.sleep(10)

        except Exception as e:
            print(f"❌ Script Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()