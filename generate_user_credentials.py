import bcrypt
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_password_from_client_info(first_name, last_name, client_id):
    """
    Generate a unique but memorable password based on client information.
    In a real system, you'd want to send this securely to the client.
    """
    # Create a unique password using the client's name and ID
    base_password = f"{first_name}{last_name}{client_id}!2025"
    return base_password

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        connection = psycopg2.connect(os.getenv('DATABASE_URL'))
        return connection
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise

def create_user_credentials():
    """
    Automatically create user credentials for all clients in the core.clients table
    using their personal information to generate unique passwords.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get all clients from the database
        query = """
            SELECT client_id, first_name, last_name
            FROM core.clients
            ORDER BY client_id
        """
        cursor.execute(query)
        clients = cursor.fetchall()
        
        # Generate credentials for each client
        for client in clients:
            client_id, first_name, last_name = client
            
            # Create a default email based on name
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            
            # Generate username from first and last name
            username = f"{first_name.lower()}{last_name.lower()}"
            
            # Generate a unique password based on client information
            password = generate_password_from_client_info(first_name, last_name, client_id)
            
            # Hash the password using bcrypt
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            # Insert the user credentials into the users table
            insert_query = """
                INSERT INTO core.users (client_id, email, username, password_hash)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE
                SET email = EXCLUDED.email, password_hash = EXCLUDED.password_hash
            """
            
            cursor.execute(insert_query, (client_id, email, username, hashed_password))
            print(f"Created/Updated user: {username}, Password: {password}")
        
        connection.commit()
        print(f"\nSuccessfully created credentials for {len(clients)} clients")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error creating user credentials: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def verify_credentials():
    """
    Verify that all clients now have credentials in the users table.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get all users from the database
        query = """
            SELECT u.client_id, u.username, c.first_name, c.last_name
            FROM core.users u
            JOIN core.clients c ON u.client_id = c.client_id
            ORDER BY u.client_id
        """
        cursor.execute(query)
        users = cursor.fetchall()
        
        print("\nCurrent users in the system:")
        for user in users:
            client_id, username, first_name, last_name = user
            print(f"Client ID: {client_id}, Name: {first_name} {last_name}, Username: {username}")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error verifying credentials: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    print("Generating user credentials based on client information...")
    create_user_credentials()
    print("\nVerification of created credentials:")
    verify_credentials()