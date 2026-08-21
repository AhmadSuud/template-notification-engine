"""
Database setup script
Executes schema.sql to create database tables
"""
import psycopg2
import logging
import os
from .config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_database():
    """Setup database schema from schema.sql file"""
    try:
        # Connect to database
        conn = psycopg2.connect(Config.get_db_connection_string())
        cursor = conn.cursor()
        
        # Read schema.sql file
        schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schema.sql')
        
        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found at: {schema_path}")
            return False
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Execute schema
        logger.info("Executing database schema...")
        cursor.execute(schema_sql)
        conn.commit()
        
        logger.info("Database schema setup completed successfully!")
        
        # Verify tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        logger.info(f"Tables in database: {[t[0] for t in tables]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False


if __name__ == '__main__':
    setup_database()
