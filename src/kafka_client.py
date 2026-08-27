from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import logging
import time
from typing import Dict, Optional, Callable
from .config import Config

logger = logging.getLogger(__name__)

# --- SKEMA AVRO ---
RAW_SCHEMA_STR = """{"type": "record", "name": "RawNotification", "namespace": "bni.notification.raw", "fields": [{"name": "ACCT_NO", "type": ["null", "string"], "default": null}, {"name": "JRNL_NO", "type": ["null", "string"], "default": null}, {"name": "TRAN_DATE", "type": ["null", "string"], "default": null}, {"name": "TRAN_TIME", "type": ["null", "string"], "default": null}, {"name": "ORIG_JRNL_NO", "type": ["null", "string"], "default": null}, {"name": "NO_HP", "type": ["null", "string"], "default": null}, {"name": "CIF", "type": ["null", "string"], "default": null}, {"name": "TRAN_TYPE", "type": ["null", "string"], "default": null}, {"name": "TRAN_CODE", "type": ["null", "string"], "default": null}, {"name": "AMOUNT", "type": ["null", "string"], "default": null}, {"name": "CURR_BAL", "type": ["null", "string"], "default": null}, {"name": "ACCT_FROM_TO", "type": ["null", "string"], "default": null}, {"name": "REVERSAL_FLAG", "type": ["null", "string"], "default": null}, {"name": "NARRATIVE_1", "type": ["null", "string"], "default": null}, {"name": "NARRATIVE_2", "type": ["null", "string"], "default": null}, {"name": "NARRATIVE_3", "type": ["null", "string"], "default": null}, {"name": "NOTIF_TYPE", "type": ["null", "string"], "default": null}, {"name": "EXTRACT_FLAG", "type": ["null", "string"], "default": null}, {"name": "FEE_FLAG", "type": ["null", "string"], "default": null}, {"name": "ROW_NO", "type": ["null", "string"], "default": null}, {"name": "TS_SRC", "type": ["null", "string"], "default": null}, {"name": "TS_KAF", "type": ["null", "string"], "default": null}, {"name": "TS_UPD", "type": ["null", "string"], "default": null}, {"name": "OP_SRC", "type": ["null", "string"], "default": null}, {"name": "OF_KAF", "type": ["null", "string"], "default": null}, {"name": "PT_KAF", "type": ["null", "string"], "default": null}, {"name": "A_CCID", "type": ["null", "string"], "default": null}, {"name": "A_CRRN", "type": ["null", "string"], "default": null}, {"name": "FL_ETL", "type": ["null", "string"], "default": null}]}"""
BROADCAST_SCHEMA_STR = """{"type": "record", "name": "BroadcastMessage", "namespace": "bni.notification", "fields": [{"name": "event_id", "type": "string"}, {"name": "channel", "type": "string"}, {"name": "template_id", "type": ["null", "string"], "default": null}, {"name": "sender", "type": "string"}, {"name": "receiver", "type": "string"}, {"name": "subject", "type": ["null", "string"], "default": null}, {"name": "body", "type": "string"}]}"""
DLQ_SCHEMA_STR = """{"type": "record", "name": "DlqMessage", "namespace": "bni.notification.dlq", "fields": [{"name": "error_reason", "type": "string"}, {"name": "raw_payload", "type": "string"}, {"name": "timestamp", "type": "double"}]}"""
STATUS_SCHEMA_STR = """{"type": "record", "name": "StatusMessage", "namespace": "bni.notification.status", "fields": [{"name": "event_id", "type": "string"}, {"name": "status", "type": "string"}, {"name": "error_message", "type": ["null", "string"], "default": null}]}"""

class KafkaConsumerClient:
    def __init__(self, topics: list = None, partition: int = None, offset: int = None):
        self.topics = topics or [Config.KAFKA_TOPICS]
        self.partition = partition
        self.offset = offset
        self.consumer = None
        self.running = False
        self._initialize_consumer()

    def _initialize_consumer(self):
        sr_conf = {'url': Config.SCHEMA_REGISTRY_URL}
        if Config.KAFKA_SECURITY_PROTOCOL == 'SSL':
            sr_conf['ssl.ca.location'] = Config.KAFKA_SSL_CA_LOCATION.replace('\\', '/')

        sr_client = SchemaRegistryClient(sr_conf)
        # Deserializer dinamis: Raw (Data Mentah) & Status (Laporan dari Broadcaster)
        self.avro_deserializer_raw = AvroDeserializer(sr_client, RAW_SCHEMA_STR, lambda obj, ctx: obj)
        self.avro_deserializer_status = AvroDeserializer(sr_client, STATUS_SCHEMA_STR, lambda obj, ctx: obj)

        consumer_config = Config.get_kafka_consumer_config()
        self.consumer = Consumer(consumer_config)

        if self.partition is not None:
            # Assign ke partisi spesifik saja, tanpa rebalance group
            offset = self.offset if self.offset is not None else 0
            assignments = [TopicPartition(topic, self.partition, offset) for topic in self.topics]
            self.consumer.assign(assignments)
            logger.info(f"Assigned partition={self.partition} offset={offset} on topics={self.topics}")
        else:
            self.consumer.subscribe(self.topics)

    def consume_messages(self, callback: Callable[[Dict, str], None], updater: Callable[[Dict], None], dlq_handler: Callable[[str, str], None], poll_timeout: float = 1.0):
        self.running = True
        logger.info("Starting Kafka Avro Consumer...")

        while self.running:
            msg = self.consumer.poll(timeout=poll_timeout)
            if msg is None or msg.error(): continue

            try:
                topic_name = msg.topic()
                # 1. Fallback untuk pesan non-Avro (misal: JSON dari templates.raw)
                if not isinstance(msg.value(), bytes) or msg.value()[0] != 0:
                    message_data = json.loads(msg.value().decode('utf-8'))
                else:
                    # 2. Deserialisasi Biner Avro berdasarkan Topik
                    if topic_name.startswith('notification.status'):
                        message_data = self.avro_deserializer_status(msg.value(), SerializationContext(topic_name, MessageField.VALUE))
                    else:
                        message_data = self.avro_deserializer_raw(msg.value(), SerializationContext(topic_name, MessageField.VALUE))
                
                if topic_name in Config.KAFKA_TOPICS.split(','):
                    callback(message_data, topic_name, msg.partition(), msg.offset())
                elif topic_name == 'templates.raw':
                    updater(message_data)
                    
                self.consumer.commit(msg)
            except Exception as e:
                logger.error(f"Error deserialisasi pesan Avro: {e}")
                
    def close(self):
        self.running = False
        if self.consumer: self.consumer.close()

class KafkaProducerClient:
    def __init__(self):
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        producer_config = Config.get_kafka_producer_config()
        self.producer = Producer(producer_config)
        
        sr_conf = {'url': Config.SCHEMA_REGISTRY_URL}
        if Config.KAFKA_SECURITY_PROTOCOL == 'SSL':
            sr_conf['ssl.ca.location'] = Config.KAFKA_SSL_CA_LOCATION.replace('\\', '/')
            
        sr_client = SchemaRegistryClient(sr_conf)
        self.ser_broadcast = AvroSerializer(sr_client, BROADCAST_SCHEMA_STR, lambda obj, ctx: obj)
        self.ser_dlq = AvroSerializer(sr_client, DLQ_SCHEMA_STR, lambda obj, ctx: obj)
        self.ser_raw = AvroSerializer(sr_client, RAW_SCHEMA_STR, lambda obj, ctx: obj)

    def send_message(self, topic: str, message: Dict, key: Optional[str] = None):
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            # Smart Routing: Pilih Serializer Avro sesuai jenis topiknya
            if topic == Config.KAFKA_DLQ_TOPIC:
                val = self.ser_dlq(message, ctx)
            elif topic == Config.KAFKA_RETRY_TOPIC:
                val = self.ser_raw(message, ctx)
            else:
                val = self.ser_broadcast(message, ctx)
                
            self.producer.produce(topic=topic, value=val, key=key.encode('utf-8') if key else None)
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send Avro message to '{topic}': {e}")
            raise

    def close(self):
        if self.producer: self.producer.flush()