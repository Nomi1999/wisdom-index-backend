from flask import jsonify
from flask_jwt_extended import create_access_token, get_jwt_identity
from database import get_db_connection, close_db_connection
import psycopg2
import bcrypt
import os
from functools import wraps

def authenticate_user(username, password):
    """
    Authenticate a user against the database.
    
    Args:
        username (str): The username or email provided by the user
        password (str): The plain text password provided by the user
        
    Returns:
        dict: User information if authentication is successful, None otherwise
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Query to find user by username or email from the new users table
        query = """
            SELECT user_id, client_id, username, email, password_hash
            FROM core.users
            WHERE username = %s OR email = %s
        """
        
        cursor.execute(query, (username, username))
        user = cursor.fetchone()
        
        cursor.close()
        close_db_connection(connection)
        
        if user:
            # Verify the password hash using bcrypt
            stored_password_hash = user[4]  # password_hash is the 5th column (index 4)
            if bcrypt.checkpw(password.encode('utf-8'), stored_password_hash.encode('utf-8')):
                return {
                    'user_id': user[0],
                    'client_id': user[1],
                    'username': user[2],
                    'email': user[3]
                }
        
        return None
        
    except psycopg2.Error as e:
        print(f"Database error during authentication: {e}")
        if connection:
            close_db_connection(connection)
        return None
    except Exception as e:
        print(f"Error during authentication: {e}")
        if connection:
            close_db_connection(connection)
        return None

def update_last_login(user_id):
    """
    Update the last_login timestamp for a user.
    
    Args:
        user_id (int): The ID of the user to update
        
    Returns:
        bool: True if successful, False otherwise
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Update the last_login field with the current timestamp
        update_query = """
            UPDATE core.users
            SET last_login = CURRENT_TIMESTAMP
            WHERE user_id = %s
        """
        
        cursor.execute(update_query, (user_id,))
        connection.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        print(f"Error updating last login for user {user_id}: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            close_db_connection(connection)

def login_user(username, password):
    """
    Process user login and return JWT token.
    
    Args:
        username (str): The username or email provided by the user
        password (str): The password provided by the user
        
    Returns:
        tuple: (response_data, status_code)
    """
    # Authenticate the user
    user = authenticate_user(username, password)
    
    if user:
        # Check if user is admin to determine token identity
        from database import get_db_connection, close_db_connection
        
        connection = get_db_connection()
        cursor = connection.cursor()
        
        cursor.execute("""
            SELECT "isAdmin", "isSuperuser" FROM core.users WHERE user_id = %s
        """, (user['user_id'],))
        
        admin_result = cursor.fetchone()
        is_admin = admin_result and admin_result[0] is True
        is_superuser = admin_result and admin_result[1] is True
        cursor.close()
        close_db_connection(connection)
        
        # Update the last_login timestamp for the user
        update_last_login(user['user_id'])
        
        # Create access token with user_id as identity for admin status checking
        # The client_id is included in the user object for data access
        token_identity = str(user['user_id']) if is_admin else str(user['client_id'])
        access_token = create_access_token(identity=token_identity)
        
        return {
            "message": "Login successful",
            "access_token": access_token,
            "user": {
                "user_id": user['user_id'],
                "client_id": user['client_id'],
                "username": user['username'],
                "email": user['email'],
                "isAdmin": is_admin,
                "isSuperuser": is_superuser
            }
        }, 200
    else:
        return {"message": "Invalid credentials"}, 401

def register_user(first_name, last_name, email, username, password):
    """
    Register a new user if they exist in the core.clients table.
    
    Args:
        first_name (str): Client's first name
        last_name (str): Client's last name
        email (str): Client's email
        username (str): Desired username
        password (str): Plain text password to be hashed
        
    Returns:
        tuple: (response_data, status_code)
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Check if the client exists in the core.clients table
        check_client_query = """
            SELECT client_id FROM core.clients
            WHERE first_name ILIKE %s AND last_name ILIKE %s
        """
        cursor.execute(check_client_query, (first_name, last_name))
        client = cursor.fetchone()
        
        if not client:
            cursor.close()
            close_db_connection(connection)
            return {"message": "Client not found in our records"}, 400
        
        client_id = client[0]
        
        # Check if a user account already exists for this client
        check_user_query = """
            SELECT user_id FROM core.users
            WHERE client_id = %s
        """
        cursor.execute(check_user_query, (client_id,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            cursor.close()
            close_db_connection(connection)
            return {"message": "An account already exists for this client"}, 409
        
        # Hash the password
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Insert the new user
        insert_user_query = """
            INSERT INTO core.users (client_id, email, username, password_hash)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_user_query, (client_id, email, username, hashed_password))
        
        connection.commit()
        cursor.close()
        close_db_connection(connection)
        
        return {"message": "Registration successful"}, 201
        
    except psycopg2.Error as e:
        print(f"Database error during registration: {e}")
        if connection:
            connection.rollback()
            close_db_connection(connection)
        
        # Check for specific constraint violations
        error_message = str(e)
        if "duplicate key value violates unique constraint" in error_message:
            if "users_email_key" in error_message:
                return {"message": "This email address is already registered"}, 409
            elif "users_username_key" in error_message:
                return {"message": "This username is already taken"}, 409
            else:
                return {"message": "Registration failed: Duplicate entry"}, 409
        else:
            return {"message": f"Database error: {error_message}"}, 500
    except Exception as e:
        print(f"Error during registration: {e}")
        if connection:
            close_db_connection(connection)
        return {"message": "Registration failed"}, 500

def is_admin_user(user_id):
    """
    Check if the authenticated user has admin privileges.
    
    Args:
        user_id (int): The user ID from JWT token
        
    Returns:
        bool: True if user is admin, False otherwise
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
            SELECT "isAdmin"
            FROM core.users
            WHERE user_id = %s
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        
        return result and result[0] is True
        
    except Exception as e:
        print(f"Error checking admin status: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            close_db_connection(connection)

def is_superuser(user_id):
    """
    Check if the authenticated user has superuser privileges.
    
    Args:
        user_id (int): The user ID from JWT token
        
    Returns:
        bool: True if user is superuser, False otherwise
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
            SELECT "isSuperuser"
            FROM core.users
            WHERE user_id = %s
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        
        return result and result[0] is True
        
    except Exception as e:
        print(f"Error checking superuser status: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            close_db_connection(connection)

def superuser_required(f):
    """
    Decorator to require superuser privileges for API endpoints.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Get user ID from JWT token
            user_id = get_jwt_identity()
            
            # Check if user is admin
            is_admin = is_admin_user(int(user_id))
            
            if not is_admin:
                return jsonify({"error": "Admin access required"}), 403
            
            # Check if user is superuser
            is_super = is_superuser(int(user_id))
            
            if not is_super:
                return jsonify({"error": "Superuser access required"}), 403
                
            return f(*args, **kwargs)
            
        except Exception as e:
            print(f"Superuser authentication error: {e}")
            return jsonify({"error": "Authentication failed"}), 401
            
    return decorated_function

def admin_required(f):
    """
    Decorator to require admin privileges for API endpoints.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Get user ID from JWT token
            user_id = get_jwt_identity()
            
            # Check if user is admin
            if not is_admin_user(int(user_id)):
                return jsonify({"error": "Admin access required"}), 403
                
            return f(*args, **kwargs)
            
        except Exception as e:
            print(f"Admin authentication error: {e}")
            return jsonify({"error": "Authentication failed"}), 401
            
    return decorated_function

def get_admin_security_code():
    """
    Get the current admin security code from the database.
    
    Returns:
        str: The current security code, or None if not found
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
            SELECT config_value FROM core.system_config
            WHERE config_key = 'admin_security_code'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result[0]
        else:
            # Fallback to environment variable if database doesn't have the code
            return os.getenv('ADMIN_SECURITY_CODE', 'WisdomAdmin2025!')
        
    except Exception as e:
        print(f"Error getting admin security code: {e}")
        # Fallback to environment variable on error
        return os.getenv('ADMIN_SECURITY_CODE', 'WisdomAdmin2025!')
    finally:
        if connection:
            close_db_connection(connection)

def update_admin_security_code(new_code, updated_by_user_id):
    """
    Update the admin security code in the database.
    
    Args:
        new_code (str): The new security code
        updated_by_user_id (int): The ID of the user who made the change
        
    Returns:
        tuple: (success: bool, message: str)
    """
    connection = None
    try:
        # Validate the new security code
        if not new_code or len(new_code) < 8:
            return False, "Security code must be at least 8 characters long"
        
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get the username of the user who is updating the security code
        cursor.execute("SELECT username FROM core.users WHERE user_id = %s", (updated_by_user_id,))
        user_result = cursor.fetchone()
        username = user_result[0] if user_result else "Unknown"
        
        # Update or insert the security code
        upsert_query = """
            INSERT INTO core.system_config (config_key, config_value, description)
            VALUES ('admin_security_code', %s, 'Security code for admin registration')
            ON CONFLICT (config_key)
            DO UPDATE SET
                config_value = %s,
                updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(upsert_query, (new_code, new_code))
        connection.commit()
        cursor.close()
        
        # Log the security code change for audit purposes
        try:
            cursor = connection.cursor()
            log_query = """
                INSERT INTO core.system_config (config_key, config_value, description)
                VALUES ('admin_security_code_updated_by', %s, 'Username who last updated security code')
                ON CONFLICT (config_key)
                DO UPDATE SET
                    config_value = %s,
                    updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(log_query, (username, username))
            connection.commit()
            cursor.close()
        except Exception as log_error:
            print(f"Error logging security code update: {log_error}")
        
        return True, "Security code updated successfully"
        
    except Exception as e:
        print(f"Error updating admin security code: {e}")
        if connection:
            connection.rollback()
        return False, f"Failed to update security code: {str(e)}"
    finally:
        if connection:
            close_db_connection(connection)

def register_admin_user(first_name, last_name, email, password, security_code):
    """
    Register a new admin user with security code validation.
    
    Args:
        first_name (str): Admin's first name
        last_name (str): Admin's last name
        email (str): Admin's email
        password (str): Plain text password to be hashed
        security_code (str): Security code for admin registration
        
    Returns:
        tuple: (response_data, status_code)
    """
    connection = None
    try:
        # Validate security code using database-stored value
        ADMIN_SECURITY_CODE = get_admin_security_code()
        
        if security_code != ADMIN_SECURITY_CODE:
            return {"error": "Invalid security code"}, 401
        
        # Validate email format
        import re
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            return {"error": "Invalid email format"}, 400
        
        # Validate password strength
        if len(password) < 8:
            return {"error": "Password must be at least 8 characters long"}, 400
        
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Check if email already exists
        cursor.execute("SELECT user_id FROM core.users WHERE email = %s", (email,))
        if cursor.fetchone():
            return {"error": "Email already registered"}, 400
        
        # Check if username already exists (we'll create username from email)
        username = email.split('@')[0]
        cursor.execute("SELECT user_id FROM core.users WHERE username = %s", (username,))
        if cursor.fetchone():
            # If username exists, add a number
            counter = 1
            while True:
                new_username = f"{username}{counter}"
                cursor.execute("SELECT user_id FROM core.users WHERE username = %s", (new_username,))
                if not cursor.fetchone():
                    username = new_username
                    break
                counter += 1
        
        # Hash the password
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Check if this is the first admin user (no existing superusers)
        cursor.execute("""
            SELECT COUNT(*) FROM core.users WHERE "isSuperuser" = true
        """)
        superuser_count = cursor.fetchone()[0]
        
        # First admin becomes superuser
        is_first_admin = superuser_count == 0
        
        # Create admin user with client_id 99 (special admin client)
        insert_query = """
            INSERT INTO core.users (client_id, email, username, password_hash, "isAdmin", "isSuperuser")
            VALUES (99, %s, %s, %s, true, %s)
            RETURNING user_id
        """
        
        cursor.execute(insert_query, (email, username, hashed_password, is_first_admin))
        user_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        
        # Create access token
        access_token = create_access_token(identity=str(user_id))
        
        return {
            "message": "Admin registration successful",
            "user": {
                "user_id": user_id,
                "email": email,
                "username": username,
                "isAdmin": True,
                "isSuperuser": is_first_admin
            },
            "access_token": access_token
        }, 201
        
    except Exception as e:
        print(f"Error during admin registration: {e}")
        if connection:
            connection.rollback()
        return {"error": "Registration failed"}, 500
    finally:
        if connection:
            close_db_connection(connection)