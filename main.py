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
    parser.add_argument('--part', type=int, nargs='+', default=None, metavar='PARTITION',
                        help='Kafka partition(s) to consume from (e.g. --part 0 1 2)')
    parser.add_argument('--offset', type=int, default=None, metavar='OFFSET',
                        help='Starting offset applied to all specified partitions (requires --part)')
    parser.add_argument('--group-id', type=str, default=None, metavar='GROUP_ID',
                        help='Kafka consumer group.id (overrides env KAFKA_GROUP_ID)')
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
        logger.info(f"Mode: partitions={args.part}, offset={args.offset}")
    if args.group_id is not None:
        logger.info(f"Consumer group.id override: {args.group_id}")
    logger.info("=" * 80)

    engine = ETLEngine()

    try:
        engine.start(partition=args.part, offset=args.offset, group_id=args.group_id)
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