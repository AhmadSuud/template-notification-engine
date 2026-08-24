"""
ETL Processor - Main processing logic
Orchestrates the flow: Kafka raw -> Template rendering -> Kafka output
"""
import logging
import time
import json
from typing import Dict, Optional
from .database import Database, NotificationTemplateRepository
from .kafka_client import KafkaProducerClient
from .template_renderer import TemplateRenderer
from .config import Config

logger = logging.getLogger(__name__)


class NotificationProcessor:
    """Main ETL processor for notification messages"""

    def __init__(self):
        """Initialize processor with database and Kafka producer"""
        self.db = Database()
        self.template_repo = NotificationTemplateRepository(self.db)
        
        from .database import NotificationLogRepository
        self.log_repo = NotificationLogRepository(self.db) 
        
        self.templates = self.template_repo.get_all_active_templates()
        self.producer = KafkaProducerClient()
        self.renderer = TemplateRenderer()

    def update_templates(self, message: Dict):
        try:
            event_type = message.get('event_type')
            template_id = message.get('template_id')
            data = message.get('data')
            del data['id']
            if event_type == 'UPDATE':
                self.templates[template_id].update(**data)
                print("Template Updated Successfully ")
            elif event_type == 'DELETE':
                del self.templates[template_id]
                print("Template Deleted Successfully ")
            elif event_type == 'CREATE':
                self.templates[template_id] = data
                print('Template Created Successfully')
        except Exception as e:
            logger.error(f"Error update templates: {e}", exc_info=True)

    # --- FITUR BARU: Fungsi untuk melempar pesan cacat ke DLQ ---
    def send_to_dlq(self, raw_value: str, error_reason: str):
        dlq_message = {
            "error_reason": error_reason,
            "raw_payload": raw_value,
            "timestamp": time.time()
        }
        self.producer.send_message(
            topic=Config.KAFKA_DLQ_TOPIC,
            message=dlq_message,
            key=None
        )
        logger.info(f"Pesan beracun dialihkan ke DLQ. Alasan: {error_reason}")

    def process_message(self, message: Dict, topic: str = None):
        """Process a single notification message or status update"""
        try:
            # --- TUGAS 1: TANGKAP PESAN STATUS DARI BROADCASTER ---
            if topic and topic.startswith('notification.status'):
                event_id = message.get('event_id')
                status = message.get('status')
                error_msg = message.get('error_message')
                
                if event_id and status:
                    self.log_repo.append_status(event_id, status, error_msg)
                    logger.info(f"Status Log Appended di Database: {event_id} -> {status}")
                return

            # --- TUGAS 2: PROSES PESAN RAW NORMAL (ETL) ---
            start_time = time.time()
            event_id = message.get('event_id')
            channel = message.get('channel')
            payload = message.get('payload', {})
            template_id = message.get('template_id')

            # Lempar ke DLQ jika event_id tidak ada
            if not event_id:
                logger.warning("Message missing event_id, routing to DLQ")
                self.send_to_dlq(json.dumps(message), "Missing event_id")
                return

            if not channel:
                logger.warning(f"Message {event_id} missing channel, skipping")
                return

            # --- FITUR BARU: Cek Idempotensi (Cegah Duplikasi Pengiriman) ---
            if hasattr(self.log_repo, 'check_event_exists') and self.log_repo.check_event_exists(event_id):
                logger.warning(f"IDEMPOTENSI: Event ID '{event_id}' sudah diproses sebelumnya. Mencegah duplikasi.")
                return

            template = self.templates.get(template_id)
            if not template:
                logger.warning(f"No active template found for id '{template_id}'")
                return

            rendered = self.renderer.render_subject_and_body(
                subject_template=template.get('subject'),
                body_template=template['body'],
                payload=payload
            )

            if rendered['body'] is None:
                logger.error(f"Failed to render body template for message {event_id}, skipping")
                return

            self.log_repo.insert_pending(
                event_id=event_id,
                template_id=template_id,
                channel=channel,
                recipient=message.get('receiver', 'unknown'),
                subject=rendered['subject'] or '',
                payload=message 
            )

            output_message = {
                'event_id': event_id,
                'channel': channel,
                'template_id': template_id,
                'sender': message.get('sender'),
                'receiver': message.get('receiver'),
                'subject': rendered['subject'],
                'body': rendered['body'],
                'payload': payload
            }

            output_topic = Config.get_topic_for_channel(channel)
            self.producer.send_message(
                topic=output_topic,
                message=output_message,
                key=event_id
            )
            elapsed = time.time() - start_time
            print(f"Total time process: {elapsed:.4f} seconds")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def close(self):
        """Close all connections"""
        logger.info("Closing NotificationProcessor...")
        self.producer.close()
        self.db.close()
        logger.info("NotificationProcessor closed")


class ETLEngine:
    """Main ETL Engine - coordinates consumer and processor"""

    def __init__(self):
        """Initialize ETL engine"""
        self.processor = NotificationProcessor()
        logger.info("ETL Engine initialized")

    def start(self):
        """Start the ETL engine"""
        from .kafka_client import KafkaConsumerClient

        # Create consumer
        consumer = KafkaConsumerClient(topics=Config.KAFKA_TOPICS.split(','))

        try:
            # Start consuming messages
            consumer.consume_messages(
                callback=self.processor.process_message,
                updater=self.processor.update_templates,
                # --- FITUR BARU: Masukkan dlq_handler ke parameter ---
                dlq_handler=self.processor.send_to_dlq,
                poll_timeout=0.1
            )
        except KeyboardInterrupt:
            logger.info("ETL Engine stopped by user")
        except Exception as e:
            logger.error(f"ETL Engine error: {e}", exc_info=True)
        finally:
            self.stop()

    def stop(self):
        """Stop the ETL engine"""
        logger.info("Stopping ETL Engine...")
        self.processor.close()
        logger.info("ETL Engine stopped")