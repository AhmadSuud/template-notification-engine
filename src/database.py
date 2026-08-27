import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json
from typing import Optional, Dict
from .config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = self.connect()
    
    def connect(self):
        try:
            return psycopg2.connect(
                Config.get_db_connection_string(),
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def commit(self):
        if self.conn:
            self.conn.commit()
    
    def rollback(self):
        if self.conn:
            self.conn.rollback()

class RetryConfigRepository:
    def __init__(self, db: Database):
        self.db = db

    def get_active_config(self) -> Optional[Dict]:
        """Mengambil konfigurasi retry yang aktif dari database"""
        try:
            cursor = self.db.conn.cursor()
            query = """
                SELECT max_retries, initial_delay_seconds, final_action 
                FROM delivery_retry_configs 
                WHERE enabled = TRUE 
                ORDER BY id ASC LIMIT 1
            """
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Gagal mengambil konfigurasi retry: {e}")
            return None

class NotificationTemplateRepository:
    def __init__(self, db: Database):
        self.db = db
    
    def get_all_active_templates(self) -> Dict:
        try:
            cursor = self.db.conn.cursor()
            query = """
                SELECT id, nama_template as name, kanal as channel, 
                       kode_segmen, subject, body
                FROM bni_notification_templates
                WHERE is_active = TRUE
                ORDER BY created_at DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            datas = {}
            for row in results:
                dict_row = dict(row)
                template_id = dict_row.pop('id')
                datas[template_id] = dict_row
            cursor.close()
            logger.info(f"Loaded {len(datas)} active templates from DB")
            return datas
        except Exception as e:
            logger.error(f"Error fetching active templates: {e}")
            return {}
        
class NotificationLogRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert_pending(self, event_id: str, template_id: Optional[str], channel: str, recipient: str, subject: str, payload: dict, retry_count: int = 0):
        try:
            cursor = self.db.conn.cursor()
            query = """
                INSERT INTO notification_logs 
                (event_id, template_id, channel, recipient, subject, status, payload, retry_count)
                VALUES (%s, %s, %s, %s, %s, 'PENDING', %s, %s)
            """
            safe_template_id = template_id if template_id and template_id != "NONE" else None
            
            # Tambahkan parameter retry_count ke dalam execute
            cursor.execute(query, (event_id, safe_template_id, channel, recipient, subject, json.dumps(payload), retry_count))
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Gagal mencatat log PENDING: {e}")
            self.db.rollback()

    def append_status(self, event_id: str, channel: str, status: str, error_message: str = None):
        try:
            cursor = self.db.conn.cursor()
            query = """
                INSERT INTO notification_logs 
                (event_id, template_id, channel, recipient, subject, payload, retry_count, status, error_message, sent_at)
                SELECT 
                    event_id, template_id, channel, recipient, subject, payload, retry_count,
                    %s, %s, CASE WHEN %s = 'SUCCESS' THEN NOW() ELSE NULL END
                FROM notification_logs
                WHERE event_id = %s AND channel = %s
                ORDER BY created_at DESC
                LIMIT 1
            """
            cursor.execute(query, (status, error_message, status, event_id, channel))
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Gagal append status log: {e}")
            self.db.rollback()

    def get_log_by_event_channel(self, event_id: str, channel: str) -> Optional[Dict]:
        """Mengambil data asli (payload & retry_count) untuk keperluan retry"""
        try:
            cursor = self.db.conn.cursor()
            query = """
                SELECT payload, COALESCE(retry_count, 0) as retry_count 
                FROM notification_logs 
                WHERE event_id = %s AND channel = %s 
                ORDER BY created_at DESC LIMIT 1
            """
            cursor.execute(query, (event_id, channel))
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Gagal mengambil log untuk retry: {e}")
            return None

    def increment_retry_count(self, event_id: str, channel: str):
        """Menambahkan nilai hitungan retry di database saat dieksekusi"""
        try:
            cursor = self.db.conn.cursor()
            query = """
                UPDATE notification_logs 
                SET retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW() 
                WHERE event_id = %s AND channel = %s
            """
            cursor.execute(query, (event_id, channel))
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Gagal increment retry count: {e}")
            self.db.rollback()

    def check_event_exists(self, event_id: str) -> bool:
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT 1 FROM notification_logs WHERE event_id = %s LIMIT 1", (event_id,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"Gagal mengecek eksistensi event: {e}")
            return False

class NasabahPreferenceRepository:
    def __init__(self, db: Database):
        self.db = db

    def get_preference_by_cif(self, cif: str) -> Optional[Dict]:
        try:
            cursor = self.db.conn.cursor()
            query = "SELECT * FROM nasabah_preferences WHERE cif = %s"
            cursor.execute(query, (cif,))
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Gagal mengambil preferensi CIF {cif}: {e}")
            return None