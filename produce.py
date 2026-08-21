# # import json
# # import time
# # import uuid
# # from confluent_kafka import Producer

# # # Konfigurasi Broker
# # BOOTSTRAP_SERVERS = "confluent.pegadaian.co.id:9092"
# # TOPIC_NAME = "notification.2raw"

# # def delivery_report(err, msg):
# #     """Callback untuk memverifikasi status pengiriman pesan"""
# #     if err is not None:
# #         print(f"❌ Gagal mengirim pesan: {err}")
# #     else:
# #         print(f"✅ Pesan terkirim ke topic '{msg.topic()}' [partition: {msg.partition()}] at offset {msg.offset()}")

# # def main():
# #     conf = {
# #         'bootstrap.servers': BOOTSTRAP_SERVERS,
# #         'client.id': 'python-notification-producer'
# #     }

# #     # Inisialisasi Producer
# #     producer = Producer(conf)

# #     # Payload uji coba berdasarkan data aktual database
# #     payloads = [
# #         # 1. Pesan Email (Account Closure Confirmation Email)
# #         {
# #             "event_id": str(uuid.uuid4()),
# #             "channel": "email",
# #             "template_id": "5ae95170-9fa7-4ee9-a2a9-3727a5a46c62", # Menggunakan UUID Asli
# #             "sender": "no-reply@pln.co.id",
# #             "receiver": "budi@example.com", # Script Anda membaca key 'receiver', BUKAN 'recipient'
# #             "payload": { # Script Anda membaca dict 'payload', BUKAN 'data'
# #                 "bank_name": "Bank Pegadaian",
# #                 "customer_name": "Budi Santoso",
# #                 "account_no": "1234567890",
# #                 "closure_date": "20-08-2026",
# #                 "final_balance": "50.000"
# #             }
# #         },
# #         # 2. Pesan WA (OTP Verification WA)
# #         {
# #             "event_id": str(uuid.uuid4()),
# #             "channel": "wa",
# #             "template_id": "8679a373-4ebf-4809-bbdf-2ae3f900a663", # Menggunakan UUID Asli
# #             "sender": "628112345678", 
# #             "receiver": "628987654321",
# #             "payload": {
# #                 "customer_name": "Siti",
# #                 "bank_name": "Bank Pegadaian",
# #                 "otp_code": "456987",
# #                 "validity_minutes": "5"
# #             }
# #         }
# #     ]

# #     print(f"Mencoba terhubung ke {BOOTSTRAP_SERVERS}...")
    
# #     try:
# #         # Loop untuk mengirim pesan
# #         for msg_payload in payloads:
# #             print(f"Mengirim event_id: {msg_payload['event_id']} (Channel: {msg_payload['channel']})")
            
# #             producer.produce(
# #                 topic=TOPIC_NAME,
# #                 key=msg_payload["event_id"],
# #                 value=json.dumps(msg_payload).encode('utf-8'),
# #                 callback=delivery_report
# #             )

# #         # Tunggu sampai semua pesan dalam buffer terkirim ke Broker
# #         producer.flush(timeout=10)

# #     except Exception as e:
# #         print(f"Terjadi error: {e}")

# # if __name__ == "__main__":
# #     main()



# import json
# import uuid
# from confluent_kafka import Producer

# # Konfigurasi Broker
# BOOTSTRAP_SERVERS = "confluent.pegadaian.co.id:9092"
# TOPIC_NAME = "notification.2raw"

# def delivery_report(err, msg):
#     """Callback untuk memverifikasi status pengiriman pesan"""
#     if err is not None:
#         print(f"❌ Gagal mengirim pesan: {err}")
#     else:
#         print(f"✅ Pesan terkirim ke topic '{msg.topic()}' [partition: {msg.partition()}] at offset {msg.offset()}")

# def main():
#     conf = {
#         'bootstrap.servers': BOOTSTRAP_SERVERS,
#         'client.id': 'python-notification-producer'
#     }

#     # Inisialisasi Producer
#     producer = Producer(conf)

#     # 3 Payload uji coba dengan template yang berbeda
#     payloads = [
#         # 1. Monthly Statement Email
#         {
#             "event_id": str(uuid.uuid4()),
#             "channel": "email",
#             "template_id": "1c191deb-509e-4f2d-8c7a-5e26c7884275", 
#             "sender": "billing@pln.co.id",
#             "receiver": "ahmad.nasabah@example.com",
#             "payload": {
#                 "customer_name": "Ahmad",
#                 "bank_name": "Bank Pegadaian",
#                 "month": "Agustus",
#                 "year": "2026",
#                 "account_no": "9876543210",
#                 "opening_balance": "7.000.000",
#                 "total_debit": "1.500.000",
#                 "total_credit": "5.000.000",
#                 "closing_balance": "10.500.000"
#             }
#         },
#         # 2. Failed Transaction WA
#         {
#             "event_id": str(uuid.uuid4()),
#             "channel": "wa",
#             "template_id": "00d0f64e-7818-4eb9-ae51-31ebc3d1ed21", 
#             "sender": "628112345678", 
#             "receiver": "628199999999",
#             "payload": {
#                 "customer_name": "Siti",
#                 "bank_name": "Bank Pegadaian",
#                 "transaction_type": "Transfer Antar Bank",
#                 "amount": "2.000.000",
#                 "failure_reason": "Saldo tidak mencukupi"
#             }
#         },
#         # 3. Promo Notification WA
#         {
#             "event_id": str(uuid.uuid4()),
#             "channel": "wa",
#             "template_id": "e010a8e4-335f-4d0a-9e17-5f51c8384bd8", 
#             "sender": "628112345678", 
#             "receiver": "628188888888",
#             "payload": {
#                 "customer_name": "Budi",
#                 "bank_name": "Bank Pegadaian",
#                 "promo_title": "Cashback Spesial 50%",
#                 "promo_description": "Dapatkan cashback 50% untuk setiap pembayaran tagihan listrik bulan ini.",
#                 "promo_period": "20 - 31 Agustus 2026"
#             }
#         }
#     ]

#     print(f"Mencoba terhubung ke broker {BOOTSTRAP_SERVERS}...")
    
#     try:
#         # Loop untuk mengirim semua pesan
#         for msg_payload in payloads:
#             print(f"Mengirim event_id: {msg_payload['event_id']} (Template ID: {msg_payload['template_id']})")
            
#             producer.produce(
#                 topic=TOPIC_NAME,
#                 key=msg_payload["event_id"],
#                 value=json.dumps(msg_payload).encode('utf-8'),
#                 callback=delivery_report
#             )

#         # Memastikan pesan keluar dari buffer
#         producer.flush(timeout=10)

#     except Exception as e:
#         print(f"Terjadi error saat produce: {e}")

# if __name__ == "__main__":
#     main()


import json
import uuid
import time
from confluent_kafka import Producer

# Konfigurasi Broker
BOOTSTRAP_SERVERS = "confluent.pegadaian.co.id:9092"
TOPIC_NAME = "notification.3raw"

def delivery_report(err, msg):
    """Callback untuk memverifikasi status pengiriman pesan"""
    if err is not None:
        print(f"❌ Gagal mengirim pesan: {err}")
    else:
        print(f"✅ Pesan terkirim ke topic '{msg.topic()}' [partition: {msg.partition()}] at offset {msg.offset()}")

def main():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'python-notification-producer'
    }

    # Inisialisasi Producer
    producer = Producer(conf)

    # Payload uji coba menggunakan Template 1
    payloads = [
        {
            "event_id": str(uuid.uuid4()),
            "channel": "email",
            "template_id": "aa425c3e-b4f1-46b7-86c5-8de99f2db399", # ID Template Pertama
            "sender": "no-reply@pegadaian.co.id",
            "receiver": "asuud2904@gmail.com", # Email tujuan sesuai request
            "payload": {
                "username": "Prab",
                "bank_name": "Bank Negara Indonesia",
                "account_type": "Tabungan Bebas",
                "date": "21 Agustus 2026"
            }
        }
    ]

    print(f"Mencoba terhubung ke broker {BOOTSTRAP_SERVERS}...")
    
    try:
        # Loop untuk mengirim pesan
        for msg_payload in payloads:
            print(f"Mengirim event_id: {msg_payload['event_id']} (Template ID: {msg_payload['template_id']})")
            
            producer.produce(
                topic=TOPIC_NAME,
                key=msg_payload["event_id"],
                value=json.dumps(msg_payload).encode('utf-8'),
                callback=delivery_report
            )

        # Memastikan pesan keluar dari buffer
        producer.flush(timeout=10)

    except Exception as e:
        print(f"Terjadi error saat produce: {e}")

if __name__ == "__main__":
    main()