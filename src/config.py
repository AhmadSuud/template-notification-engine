"""
Configuration module for ETL Notification Engine
Loads environment variables and provides configuration settings
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Application Configuration"""
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
    KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET')
    
    # Kafka Topics
    KAFKA_TOPICS = os.getenv('KAFKA_TOPICS')
    KAFKA_TOPIC_EMAIL = os.getenv('KAFKA_TOPIC_EMAIL')
    KAFKA_TOPIC_WA = os.getenv('KAFKA_TOPIC_WA')
    KAFKA_TOPIC_SMS = os.getenv('KAFKA_TOPIC_SMS')
    KAFKA_DLQ_TOPIC = os.getenv('KAFKA_DLQ_TOPIC')
    
    # PostgreSQL Configuration
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    # Application Settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_db_connection_string(cls):
        return f"host={cls.DB_HOST} port={cls.DB_PORT} dbname={cls.DB_NAME} user={cls.DB_USER} password={cls.DB_PASSWORD}"
    
    @classmethod
    def get_kafka_consumer_config(cls):
        return {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': cls.KAFKA_GROUP_ID,
            'auto.offset.reset': cls.KAFKA_AUTO_OFFSET_RESET,
            'enable.auto.commit': False # Manual commit untuk mencegah data hilang
        }
    
    @classmethod
    def get_kafka_producer_config(cls):
        return {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
        }
    
    @classmethod
    def get_topic_for_channel(cls, channel: str) -> str:
        channel_topics = {
            'email': cls.KAFKA_TOPIC_EMAIL,
            'wa': cls.KAFKA_TOPIC_WA,
            'sms': cls.KAFKA_TOPIC_SMS
        }
        return channel_topics.get(channel.lower(), f'notification.{channel.lower()}')