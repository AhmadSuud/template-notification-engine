"""
Database module for PostgreSQL connection and notification template operations
"""
import time

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Optional, Dict, List
from .config import Config

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database connection handler"""
    
    def __init__(self):
        self.conn = self.connect()
        self.cursor = self.conn.cursor()
    
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            return psycopg2.connect(
                Config.get_db_connection_string(),
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def commit(self):
        """Commit transaction"""
        if self.conn:
            self.conn.commit()
    
    def rollback(self):
        """Rollback transaction"""
        if self.conn:
            self.conn.rollback()


class NotificationTemplateRepository:
    """Repository for notification template operations"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_active_template_by_channel(self, channel: str) -> Optional[Dict]:
        """
        Get active template for a specific channel
        
        Args:
            channel: Channel name (email, wa, etc.)
            
        Returns:
            Template dictionary or None if not found
        """
        try:
            cursor = self.db.get_cursor()
            query = """
                SELECT id, name, channel, subject, body, variables, metadatas
                FROM notification_templates_2
                WHERE channel = %s 
                  AND is_active = TRUE 
                  AND deleted_at IS NULL
                ORDER BY created_at DESC
                LIMIT 1
            """
            cursor.execute(query, (channel,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.debug(f"Found active template for channel '{channel}': {result['name']}")
            else:
                logger.warning(f"No active template found for channel '{channel}'")
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Error fetching template for channel '{channel}': {e}")
            return None
    
    def get_all_active_templates(self) -> Dict:
        """
        Get all active templates
        
        Returns:
            List of template dictionaries
        """
        try:
            cursor = self.db.cursor
            query = """
                SELECT id, name, channel, subject, body, variables, metadatas
                FROM notification_templates_2
                WHERE is_active = TRUE AND deleted_at IS NULL
                ORDER BY channel, created_at DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            datas = {}
            logger.info(f"Found {len(results)} active templates")
            for row in results:
                dict_row = dict(row)
                template_id = dict_row.pop('id')
                datas[template_id] = dict_row
            return datas
        except Exception as e:
            logger.error(f"Error fetching active templates: {e}")
            return {}
        
class NotificationLogRepository:
    """Repository untuk mencatat riwayat pengiriman notifikasi (Append-Only/Audit Trail)"""
    
    def __init__(self, db: Database):
        self.db = db

    def insert_pending(self, event_id: str, template_id: str, channel: str, recipient: str, subject: str, payload: dict):
        """Mencatat status PENDING sebagai baris (record) baru di database"""
        try:
            cursor = self.db.conn.cursor()
            query = """
                INSERT INTO notification_logs 
                (event_id, template_id, channel, recipient, subject, status, payload)
                VALUES (%s, %s, %s, %s, %s, 'PENDING', %s)
            """
            import json
            cursor.execute(query, (event_id, template_id, channel, recipient, subject, json.dumps(payload)))
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Gagal mencatat log PENDING: {e}")
            self.db.rollback()

    def append_status(self, event_id: str, status: str, error_message: str = None):
        """Menambahkan baris baru untuk status SUCCESS/FAILED dengan menyalin info penerima dari log sebelumnya"""
        try:
            cursor = self.db.conn.cursor()
            query = """
                INSERT INTO notification_logs 
                (event_id, template_id, channel, recipient, subject, payload, status, error_message, sent_at)
                SELECT 
                    event_id, template_id, channel, recipient, subject, payload, 
                    %s, %s, CASE WHEN %s = 'SUCCESS' THEN NOW() ELSE NULL END
                FROM notification_logs
                WHERE event_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """
            cursor.execute(query, (status, error_message, status, event_id))
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Gagal append status log: {e}")
            self.db.rollback()
    def check_event_exists(self, event_id: str) -> bool:
        """Mengecek apakah event_id sudah ada untuk mencegah duplikasi (Idempotensi)"""
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT 1 FROM notification_logs WHERE event_id = %s LIMIT 1", (event_id,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"Gagal mengecek eksistensi event: {e}")
            return False

