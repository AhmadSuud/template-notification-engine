import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
    KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    # SSL & Security
    KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL')
    KAFKA_SSL_CA_LOCATION = os.getenv('KAFKA_SSL_CA_LOCATION')
    SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
    
    # Topics
    KAFKA_TOPICS = os.getenv('KAFKA_TOPICS')
    KAFKA_TOPIC_EMAIL = os.getenv('KAFKA_TOPIC_EMAIL')
    KAFKA_TOPIC_WA = os.getenv('KAFKA_TOPIC_WA')
    KAFKA_TOPIC_SMS = os.getenv('KAFKA_TOPIC_SMS')
    KAFKA_DLQ_TOPIC = os.getenv('KAFKA_DLQ_TOPIC')
    KAFKA_RETRY_TOPIC = os.getenv('KAFKA_RETRY_TOPIC')
    
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_db_connection_string(cls):
        return f"host={cls.DB_HOST} port={cls.DB_PORT} dbname={cls.DB_NAME} user={cls.DB_USER} password={cls.DB_PASSWORD}"
    
    @classmethod
    def _get_base_kafka_config(cls):
        conf = {'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS}
        if cls.KAFKA_SECURITY_PROTOCOL:
            conf['security.protocol'] = cls.KAFKA_SECURITY_PROTOCOL
        if cls.KAFKA_SSL_CA_LOCATION:
            conf['ssl.ca.location'] = cls.KAFKA_SSL_CA_LOCATION.replace('\\', '/')
        return conf
    
    @classmethod
    def get_kafka_consumer_config(cls):
        conf = cls._get_base_kafka_config()
        conf.update({'group.id': cls.KAFKA_GROUP_ID, 'auto.offset.reset': cls.KAFKA_AUTO_OFFSET_RESET, 'enable.auto.commit': False})
        return conf
        
    @classmethod
    def get_kafka_producer_config(cls):
        conf = cls._get_base_kafka_config()
        conf.update({'compression.type': 'zstd', 'linger.ms': 5, 'batch.size': 32768})
        return conf
    
    @classmethod
    def get_topic_for_channel(cls, channel: str) -> str:
        channel_topics = {'email': cls.KAFKA_TOPIC_EMAIL, 'wa': cls.KAFKA_TOPIC_WA, 'sms': cls.KAFKA_TOPIC_SMS}
        return channel_topics.get(channel.lower(), f'notification.{channel.lower()}')