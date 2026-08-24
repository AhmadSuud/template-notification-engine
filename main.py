"""
ETL Notification Engine - Main Entry Point
"""
import logging
import sys
from src.processor import ETLEngine
from src.config import Config

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_engine.log')
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for ETL Notification Engine"""
    logger.info("=" * 80)
    logger.info("ETL Notification Engine Starting")
    logger.info("=" * 80)
    logger.info(f"Kafka Bootstrap Servers: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Consumer Group: {Config.KAFKA_GROUP_ID}")
    logger.info(f"Consuming from topics: {Config.KAFKA_TOPICS}")
    logger.info(f"Database: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
    logger.info("=" * 80)
    
    # Create and start ETL engine
    engine = ETLEngine()
    
    try:
        engine.start()
    except KeyboardInterrupt:
        logger.info("\nShutdown signal received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=" * 80)
        logger.info("ETL Notification Engine Stopped")
        logger.info("=" * 80)

if __name__ == '__main__':
    main()