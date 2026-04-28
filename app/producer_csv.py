import time
import json
import csv
import datetime
from kafka import KafkaProducer
import os

# --- НАСТРОЙКИ ---
KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "social_data"
# Имя твоего нового файла
CSV_FILE_PATH = "/app/real_tweets.csv" 

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
    print(f"🚀 Starting Real-Time Stream from {CSV_FILE_PATH}...")
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ Error: File {CSV_FILE_PATH} not found! (Did you put real_tweets.csv in /app?)")
        return

    while True:
        with open(CSV_FILE_PATH, "r", encoding="utf-8", errors='replace') as f:
            reader = csv.reader(f)
            next(reader) 
            
            for row in reader:
                try:
                
                    if len(row) < 10: continue
                    
                    raw_date = row[8]
                    text = row[9]
                    
                    if not text or len(text) < 10:
                        continue
                    try:
                        msg_date = raw_date
                    except:
                        msg_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    msg = {
                        "source": "Twitter (Bitcoin Dataset)",
                        "text": text,
                        "created_at": msg_date  
                    }
                    
                    producer.send(KAFKA_TOPIC, msg)
                    
                    if hash(text) % 100 == 0:
                        print(f"[Old Tweet] Date: {msg_date} | Text: {text[:40]}...")
                    
                except Exception as e:
                    continue
        
        print("🔄 Dataset finished. Restarting loop...")

if __name__ == "__main__":
    main()