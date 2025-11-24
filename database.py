import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """
    Creates and returns a PostgreSQL database connection using environment variables.
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        # Add connection timeout and SSL configuration for better reliability
        connection = psycopg2.connect(
            os.getenv('DATABASE_URL'),
            connect_timeout=30,  # 30 second timeout
            sslmode='require',    # Explicit SSL requirement
            sslcert=None,         # Use default SSL certificate handling
            sslkey=None,
            sslrootcert=None
        )
        return connection
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise

def close_db_connection(connection):
    """
    Safely closes a database connection.
    
    Args:
        connection: Database connection object to close
    """
    if connection:
        connection.close()

def test_db_connection():
    """
    Test database connectivity and return status information.
    
    Returns:
        dict: Connection status and information
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Test basic query
        cursor.execute('SELECT version();')
        version = cursor.fetchone()
        
        # Test core schema access
        cursor.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s;', ('core',))
        table_count = cursor.fetchone()
        
        cursor.close()
        close_db_connection(connection)
        
        return {
            'status': 'success',
            'version': version[0] if version else 'Unknown',
            'core_tables': table_count[0] if table_count else 0,
            'message': 'Database connection is working properly'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'message': 'Database connection failed'
        }