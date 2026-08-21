"""
Kafka client module for consuming and producing messages
Uses Confluent Kafka Python client (>=2.3.0)
"""
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import json
import logging
from typing import Dict, Optional, Callable
from .config import Config
import time

logger = logging.getLogger(__name__)


class KafkaConsumerClient:
    """Kafka consumer for reading messages from topics"""

    def __init__(self, topics: list = None):
        """
        Initialize Kafka consumer
        
        Args:
            topics: List of topics to subscribe to (default: [notification.raw])
        """
        self.topics = topics or [Config.KAFKA_TOPICS]
        self.consumer = None
        self.running = False
        self._initialize_consumer()

    def _initialize_consumer(self):
        """Initialize Confluent Kafka consumer"""
        try:
            consumer_config = Config.get_kafka_consumer_config()
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Kafka consumer initialized and subscribed to topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def consume_messages(self, callback: Callable[[Dict], None], updater: Callable[[Dict], None], poll_timeout: float = 1.0):

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
                        # End of partition event
                        logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        # Error
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    message_value = msg.value().decode('utf-8')
                    message_data = json.loads(message_value)
                    
                    # if msg.topic() in Config.KAFKA_TOPICS.split(','):
                    #     start_time_callback = time.time()
                    #     callback(message_data)
                    #     end_time_callback = time.time()
                    #     logger.debug(f"Callback processing time: {end_time_callback - start_time_callback:.4f} seconds")
                    if msg.topic() in Config.KAFKA_TOPICS.split(','):
                        start_time_callback = time.time()
                        # TAMBAHAN: Kirimkan msg.topic() ke fungsi process_message
                        callback(message_data, msg.topic()) 
                        end_time_callback = time.time()
                        logger.debug(f"Callback processing time: {end_time_callback - start_time_callback:.4f} seconds")
                    elif msg.topic() == 'templates.raw':
                        updater(message_data)
                        
                    end_time = time.time()
                    logger.debug(f"Message processed in {end_time - start_time:.4f} seconds")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message as JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
        finally:
            self.close()

    def close(self):
        """Close Kafka consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


class KafkaProducerClient:
    """Kafka producer for sending messages to topics"""

    def __init__(self):
        """Initialize Kafka producer"""
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        """Initialize Confluent Kafka producer"""
        try:
            producer_config = Config.get_kafka_producer_config()
            self.producer = Producer(producer_config)
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _delivery_report(self, err, msg):
        """
        Delivery report callback for producer
        Called once for each message produced to indicate delivery result
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def send_message(self, topic: str, message: Dict, key: Optional[str] = None):
        """
        Send message to Kafka topic
        
        Args:
            topic: Target topic name
            message: Message payload as dictionary
            key: Optional message key for partitioning
        """
        try:
            # Convert message to JSON
            message_json = json.dumps(message)

            # Produce message
            self.producer.produce(
                topic=topic,
                value=message_json.encode('utf-8'),
                key=key.encode('utf-8') if key else None,
                callback=self._delivery_report
            )

            # Trigger delivery reports
            self.producer.poll(0)

            logger.debug(f"Message queued for topic '{topic}': {message.get('event_id')}")

        except Exception as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            raise

    def flush(self, timeout: float = 10.0):
        """
        Wait for all messages in the producer queue to be delivered
        
        Args:
            timeout: Maximum time to wait in seconds
        """
        if self.producer:
            remaining = self.producer.flush(timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered before timeout")

    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.flush()
            logger.info("Kafka producer closed")
