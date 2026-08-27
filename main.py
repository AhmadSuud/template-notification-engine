"""
ETL Notification Engine - Main Entry Point
"""
import argparse
import logging
import sys
from src.processor import ETLEngine
from src.config import Config

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_engine.log')
    ]
)

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description='ETL Notification Engine')
    parser.add_argument('--part', type=int, default=None, metavar='PARTITION',
                        help='Kafka partition number to consume from')
    parser.add_argument('--offset', type=int, default=None, metavar='OFFSET',
                        help='Starting offset (requires --part)')
    args = parser.parse_args()

    if args.offset is not None and args.part is None:
        parser.error('--offset requires --part')

    return args

def main():
    args = parse_args()

    logger.info("=" * 80)
    logger.info("ETL Notification Engine Starting (BNI Final Version)")
    logger.info("=" * 80)
    logger.info(f"Kafka Bootstrap Servers: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Consumer Group: {Config.KAFKA_GROUP_ID}")
    logger.info(f"Consuming from topics: {Config.KAFKA_TOPICS}")
    logger.info(f"Database: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
    if args.part is not None:
        logger.info(f"Mode: partition={args.part}, offset={args.offset}")
    logger.info("=" * 80)

    engine = ETLEngine()

    try:
        engine.start(partition=args.part, offset=args.offset)
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