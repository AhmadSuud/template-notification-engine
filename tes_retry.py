import json
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer

# --- Konfigurasi ---
DB_HOST = '10.10.8.60'
DB_PORT = '5432'
DB_NAME = 'pln_db'
DB_USER = 'pln_user'
DB_PASSWORD = 'pln_pass_12#$'

KAFKA_BROKER = 'confluent.pegadaian.co.id:9092'
KAFKA_TOPIC = 'notification.3raw' # Topik awal ETL Anda

def retry_failed_notification(event_id_to_retry):
    print(f"1. Mencari history log untuk event_id: '{event_id_to_retry}'...")
    
    # Koneksi Database
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()
        
        # Ambil payload dari record terbaru untuk event_id tersebut
        cursor.execute("""
            SELECT payload FROM notification_logs 
            WHERE event_id = %s AND payload IS NOT NULL
            ORDER BY created_at DESC LIMIT 1
        """, (event_id_to_retry,))
        
        record = cursor.fetchone()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Gagal koneksi ke database: {e}")
        return

    if not record or not record.get('payload'):
        print("❌ GAGAL: Payload asli tidak ditemukan di database!")
        return

    original_payload = record['payload']
    print(f"✅ Payload ditemukan! Mengirim ulang ke Kafka topik '{KAFKA_TOPIC}'...")

    # Tembak kembali ke Kafka
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(original_payload).encode('utf-8'),
            key=event_id_to_retry.encode('utf-8')
        )
        producer.flush()
        print("✅ Retry sukses dikirim ke antrean Kafka!")
    except Exception as e:
        print(f"❌ Gagal mengirim ke Kafka: {e}")

if __name__ == "__main__":
    # TODO: Ganti value di bawah ini dengan event_id yang ingin Anda retry!
    # Anda bisa mengeceknya langsung dari database PostgreSQL (tabel notification_logs)
    target_event_id = "ee6f8ca5-b03a-4d10-9ca7-23e845a75b83" 
    
    retry_failed_notification(target_event_id)