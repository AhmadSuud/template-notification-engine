import logging
import time
import json
import datetime
import threading
from typing import Dict, Optional
from .database import Database, NotificationTemplateRepository, NotificationLogRepository, NasabahPreferenceRepository, RetryConfigRepository
from .kafka_client import KafkaProducerClient
from .template_renderer import TemplateRenderer
from .config import Config

logger = logging.getLogger(__name__)

class NotificationProcessor:
    def __init__(self):
        self.db = Database()
        self.template_repo = NotificationTemplateRepository(self.db)
        self.log_repo = NotificationLogRepository(self.db) 
        self.pref_repo = NasabahPreferenceRepository(self.db)
        self.retry_repo = RetryConfigRepository(self.db)
        
        self.templates = self.template_repo.get_all_active_templates()
        self.producer = KafkaProducerClient()
        self.renderer = TemplateRenderer()
        self.retry_config = self.retry_repo.get_active_config() or {'max_retries': 0, 'initial_delay_seconds': 0, 'final_action': 'drop'}

    def update_templates(self, message: Dict):
        try:
            logger.info("Update templates diterima. Memperbarui cache...")
            self.templates = self.template_repo.get_all_active_templates()
            self.retry_config = self.retry_repo.get_active_config() or self.retry_config
        except Exception as e:
            logger.error(f"Error update templates: {e}")

    def send_to_dlq(self, raw_value: dict, error_reason: str):
        # raw_payload di Avro meminta String. Kita jadikan string jika yang masuk berupa dict
        raw_str = json.dumps(raw_value) if isinstance(raw_value, dict) else str(raw_value)
        dlq_message = {
            "error_reason": error_reason,
            "raw_payload": raw_str,
            "timestamp": time.time()
        }
        self.producer.send_message(topic=Config.KAFKA_DLQ_TOPIC, message=dlq_message, key=None)
        logger.info(f"Pesan dialihkan ke DLQ: {error_reason}")

    def _schedule_retry(self, delay: int, payload: Dict, event_id: str, channel: str):
        def _retry_task():
            logger.info(f"Mengeksekusi Retry Terjadwal untuk {event_id} ({channel}) setelah delay {delay} detik.")
            self.log_repo.increment_retry_count(event_id, channel)
            kafka_key = str(payload.get('CIF', ''))
            # Payload sudah berupa dict murni, langsung kirim ke KafkaProducerClient (Avro)
            self.producer.send_message(Config.KAFKA_RETRY_TOPIC, payload, kafka_key)
        
        threading.Timer(delay, _retry_task).start()

    def format_bni_variables(self, msg: Dict, pref: Dict) -> Dict:
        # (Logika format variabel sama persis seperti sebelumnya)
        formatted = dict(msg)
        try:
            actual_date = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=int(msg.get('TRAN_DATE', 0)))
            formatted['f_tanggal'] = actual_date.strftime("%d/%m/%Y")
        except: formatted['f_tanggal'] = ""

        time_str = str(msg.get('TRAN_TIME', '')).strip().zfill(8)
        formatted['f_waktu'] = f"{time_str[:2]}:{time_str[2:4]}" if len(time_str) >= 4 else ""

        acct_no = str(msg.get('ACCT_NO', ''))
        formatted['f_rek'] = acct_no[-6:] if len(acct_no) >= 6 else acct_no

        tran_type = str(msg.get('TRAN_TYPE', '')).strip().upper()
        if tran_type == 'C': formatted['f_arah'] = "ada dana masuk sebesar"
        elif tran_type == 'D': formatted['f_arah'] = "ada dana keluar sebesar"
        else: formatted['f_arah'] = ""

        def format_idr(amount):
            try: return "IDR" + f"{float(amount):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except: return "IDR0,00"
        
        try: amount_val = float(msg.get('AMOUNT', 0))
        except: amount_val = 0.0
        try: bal_val = float(msg.get('CURR_BAL', 0))
        except: bal_val = 0.0

        formatted['f_nominal'] = format_idr(amount_val)
        formatted['f_saldo'] = format_idr(bal_val)
        formatted['f_berita'] = " ".join(filter(None, [str(msg.get('NARRATIVE_1', '')).strip(), str(msg.get('NARRATIVE_2', '')).strip(), str(msg.get('NARRATIVE_3', '')).strip()]))

        segmen = pref.get('kode_segmen', '')
        if segmen == 'SEG-A': formatted['f_sapaan'] = "Yth. Nasabah Prioritas,"
        elif segmen == 'SEG-B': formatted['f_sapaan'] = "Halo Nasabah BNI,"
        else: formatted['f_sapaan'] = "Yth. Nasabah,"

        rev_flag = str(msg.get('REVERSAL_FLAG', ''))
        orig_jrnl = str(msg.get('ORIG_JRNL_NO', '')).strip()
        
        formatted['is_reversal'] = (rev_flag == '1' and orig_jrnl != '000000' and bool(orig_jrnl))
        formatted['is_non_finansial'] = (tran_type == 'H' and amount_val == 0 and bal_val == 0 and rev_flag == ' ')
        return formatted

    def process_message(self, message: Dict, topic: str = None, partition: int = None, offset: int = None):
        try:
            if topic and topic.startswith('notification.status'):
                event_id = message.get('event_id')
                status = message.get('status')
                channel_from_topic = topic.split('.')[-1] 
                
                if event_id and status:
                    if status == 'SUCCESS':
                        self.log_repo.append_status(event_id, channel_from_topic, status, message.get('error_message'))
                        logger.info(f"Status Appended: {event_id} ({channel_from_topic}) -> SUCCESS")
                    elif status == 'FAILED':
                        log_entry = self.log_repo.get_log_by_event_channel(event_id, channel_from_topic)
                        if log_entry:
                            current_retry = log_entry.get('retry_count', 0)
                            max_retries = self.retry_config.get('max_retries', 3)
                            if current_retry < max_retries:
                                delay = self.retry_config.get('initial_delay_seconds', 5)
                                self.log_repo.append_status(event_id, channel_from_topic, 'RETRYING', message.get('error_message'))
                                self._schedule_retry(delay, log_entry['payload'], event_id, channel_from_topic)
                            else:
                                final_act = self.retry_config.get('final_action')
                                self.log_repo.append_status(event_id, channel_from_topic, 'FAILED_FINAL', message.get('error_message'))
                                if final_act == 'dead_letter':
                                    self.send_to_dlq(log_entry['payload'], f"Max Retries Terlampaui untuk {channel_from_topic}")
                return

            start_time = time.time()
            
            acct_no = str(message.get('ACCT_NO', '')).strip()
            jrnl_no = str(message.get('JRNL_NO', '')).strip()
            tran_date = str(message.get('TRAN_DATE', '')).strip()
            tran_time = str(message.get('TRAN_TIME', '')).strip()
            cif = str(message.get('CIF', '')).strip()

            if not acct_no or not jrnl_no or not tran_date or not tran_time:
                self.send_to_dlq(message, "Missing Natural Key (ACCT_NO/JRNL_NO/TRAN_DATE/TRAN_TIME)")
                return
                
            event_id = f"{acct_no}-{jrnl_no}-{tran_date}-{tran_time}"
            is_retry_event = (topic == Config.KAFKA_RETRY_TOPIC)
            
            if not is_retry_event and self.log_repo.check_event_exists(event_id):
                logger.warning(f"IDEMPOTENSI: event dari partition: {partition} offset: {offset} event_id: {event_id} duplikat. Ditolak.")
                return

            pref = self.pref_repo.get_preference_by_cif(cif) if cif else None
            if not pref:
                logger.warning(f"CIF {cif} tidak ditemukan.")
                return

            if str(pref.get('status_langganan')) == '0':
                self.log_repo.insert_pending(event_id, None, "skipped", cif, "UNSUBSCRIBED", message)
                self.log_repo.append_status(event_id, "skipped", "DITOLAK", "Berhenti berlangganan")
                return

            enrich_payload = self.format_bni_variables(message, pref)
            nasabah_segmen = pref.get('kode_segmen') or 'DEFAULT'
            kanals = [k.strip().lower() for k in pref.get('kanal_preferensi', '').split(';')] 
            
            for channel in kanals:
                receiver = pref.get('email') if channel == 'email' else pref.get('no_hp')
                if not receiver: continue
                
                active_template = next((t for t in self.templates.values() if t.get('channel') == channel and t.get('kode_segmen') == nasabah_segmen), None)
                if not active_template:
                    active_template = next((t for t in self.templates.values() if t.get('channel') == channel and t.get('kode_segmen') == 'DEFAULT'), None)
                            
                if not active_template: continue
                
                # Temukan Template ID (Key) berdasarkan value-nya
                template_id = list(self.templates.keys())[list(self.templates.values()).index(active_template)]
                
                rendered = self.renderer.render_subject_and_body(
                    subject_template=active_template.get('subject'),
                    body_template=active_template['body'],
                    payload=enrich_payload
                )
                
                current_retry = self.log_repo.get_log_by_event_channel(event_id, channel).get('retry_count', 0) if is_retry_event else 0

                self.log_repo.insert_pending(event_id, str(template_id), channel, receiver, rendered['subject'] or '', message, current_retry)

                # Dictionary langsung dikirim tanpa json.dumps()
                output_message = {
                    'event_id': event_id,
                    'channel': channel,
                    'template_id': str(template_id),
                    'sender': 'BNI_NOTIF',   
                    'receiver': receiver,
                    'subject': rendered['subject'],
                    'body': rendered['body']
                }

                self.producer.send_message(topic=Config.get_topic_for_channel(channel), message=output_message, key=cif)
            
            logger.info(f"Process event dari partition: {partition} offset: {offset} event_id: {event_id} selesai dlm {time.time() - start_time:.4f} detik")
            
        except Exception as e:
            logger.error(f"Error proses pesan: {e}", exc_info=True)
            self.send_to_dlq(message, f"Eksepsi: {str(e)}")

    def close(self):
        self.producer.close()
        self.db.close()

class ETLEngine:
    def __init__(self):
        self.processor = NotificationProcessor()

    def start(self, partition=None, offset: int = None, group_id: str = None):
        from .kafka_client import KafkaConsumerClient
        consumer = KafkaConsumerClient(topics=Config.KAFKA_TOPICS.split(','), partition=partition, offset=offset, group_id=group_id)
        try:
            consumer.consume_messages(callback=self.processor.process_message, updater=self.processor.update_templates, dlq_handler=self.processor.send_to_dlq, poll_timeout=0.1)
        except KeyboardInterrupt: pass
        finally: self.stop()

    def stop(self):
        self.processor.close()