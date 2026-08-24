"""
Kafka client module for consuming and producing messages
Uses Confluent Kafka Python client (>=2.3.0)
"""
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
from typing import Dict, Optional, Callable
from .config import Config
import time

logger = logging.getLogger(__name__)

class KafkaConsumerClient:
    def __init__(self, topics: list = None):
        self.topics = topics or [Config.KAFKA_TOPICS]
        self.consumer = None
        self.running = False
        self._initialize_consumer()

    def _initialize_consumer(self):
        try:
            consumer_config = Config.get_kafka_consumer_config()
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Kafka consumer initialized and subscribed to topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def consume_messages(self, callback: Callable[[Dict, str], None], updater: Callable[[Dict], None], dlq_handler: Callable[[str, str], None], poll_timeout: float = 1.0):
        self.running = True
        logger.info("Starting Kafka consumer loop...")

        try:
            while self.running:
                start_time = time.time()
                msg = self.consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    message_value = msg.value().decode('utf-8')
                    
                    # Validasi JSON. Jika rusak, lempar ke DLQ
                    try:
                        message_data = json.loads(message_value)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON Rusak. Melempar ke DLQ: {e}")
                        dlq_handler(message_value, f"JSON Parse Error: {str(e)}")
                        self.consumer.commit(msg) 
                        continue
                    
                    if msg.topic() in Config.KAFKA_TOPICS.split(','):
                        callback(message_data, msg.topic())
                    elif msg.topic() == 'templates.raw':
                        updater(message_data)
                        
                    # Manual Commit SETELAH pesan sukses diproses
                    self.consumer.commit(msg)
                        
                    end_time = time.time()
                    logger.debug(f"Message processed in {end_time - start_time:.4f} seconds")

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
        finally:
            self.close()

    def close(self):
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

class KafkaProducerClient:
    def __init__(self):
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        try:
            producer_config = Config.get_kafka_producer_config()
            self.producer = Producer(producer_config)
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def send_message(self, topic: str, message: Dict, key: Optional[str] = None):
        try:
            message_json = json.dumps(message)
            self.producer.produce(
                topic=topic,
                value=message_json.encode('utf-8'),
                key=key.encode('utf-8') if key else None,
                callback=self._delivery_report
            )
            self.producer.poll(0)
            logger.debug(f"Message queued for topic '{topic}': {message.get('event_id')}")
        except Exception as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            raise

    def flush(self, timeout: float = 10.0):
        if self.producer:
            remaining = self.producer.flush(timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered before timeout")

    def close(self):
        if self.producer:
            self.flush()
            logger.info("Kafka producer closed")