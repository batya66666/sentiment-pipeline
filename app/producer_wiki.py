import json
import sseclient
import requests
from kafka import KafkaProducer
import datetime
import time

KAFKA_TOPIC = "social_data"
KAFKA_SERVER = "kafka:9092"

# --- ФИЛЬТР КЛЮЧЕВЫХ СЛОВ ---
FILTER_KEYWORDS = [
    "python", "java", "code", "software", "bug",          # Tech / Dev
    "ai", "gpt", "intelligence", "robot",                 # AI
    "crypto", "bitcoin", "ethereum", "blockchain", "nft", # Crypto
    "google", "microsoft", "apple", "tesla", "amazon",    # Big Tech
    "game", "playstation", "xbox", "nintendo", "steam",   # Gaming
    "movie", "film", "series", "netflix",                 # Media
    "politics", "election", "president", "war", "law",    # Politics
    "economy", "market", "money", "stock",                # Finance
    "sport", "football", "soccer", "nba"                  # Sports
]

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
    
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    headers = {
        'User-Agent': 'SentimentAnalysisBot/1.0 (student_project_coursework)'
    }
    
    print(f"🚀 Connecting to Wikipedia Stream (Filtered: {len(FILTER_KEYWORDS)} keywords)...")
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        client = sseclient.SSEClient(response)
        
        print("✅ Stream connected! Waiting for relevant edits...")
        
        for event in client.events():
            if event.event == 'message':
                try:
                    change = json.loads(event.data)
                    
                    if (change.get('server_name') == 'en.wikipedia.org' and 
                        change.get('type') == 'edit' and 
                        change.get('comment')):
                        
                        text = change['comment'].lower()
                        title = change.get('title', '').lower()
                        full_check_text = f"{title} {text}"
                        
                        if not any(word in full_check_text for word in FILTER_KEYWORDS):
                            continue

                        final_text = f"Article [{change['title']}]: {change['comment']}"

                        data = {
                            "source": "Wikipedia (Filtered)",
                            "text": final_text,
                            "created_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        producer.send(KAFKA_TOPIC, data)
                        print(f"[Wiki] Match: {change['title']} -> {change['comment'][:40]}...")
                        
                except Exception as loop_e:
                    continue
                    
    except Exception as e:
        print(f"❌ Stream Error: {e}")

if __name__ == "__main__":
    main()