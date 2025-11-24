import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the Python path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

class AuthMetricsTestCase(unittest.TestCase):
    """
    Test case for the authentication and metrics functionality.
    """

    def setUp(self):
        """
        Set up test client and other test fixtures.
        """
        self.app = app.test_client()
        self.app.testing = True

    def test_health_check(self):
        """
        Test the health check endpoint.
        """
        response = self.app.get('/health')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['status'], 'healthy')
        self.assertEqual(data['service'], 'backend_beta')

    def test_protected_net_worth_endpoint_without_auth(self):
        """
        Test that the net worth endpoint requires authentication.
        """
        response = self.app.get('/api/metrics/net_worth')
        self.assertEqual(response.status_code, 401)  # Unauthorized

        data = response.get_json()
        self.assertIn('msg', data)  # JWT extension returns 'msg' for auth errors

    def test_login_endpoint_missing_credentials(self):
        """
        Test error handling for missing credentials in login.
        """
        response = self.app.post('/auth/login', json={})
        self.assertEqual(response.status_code, 400)

        data = response.get_json()
        self.assertIn('error', data)
        self.assertEqual(data['error'], 'Username and password are required')

    def test_login_endpoint_valid_credentials_format(self):
        """
        Test login endpoint with valid credential format (but not necessarily valid credentials).
        """
        with patch('app.login_user') as mock_login:
            mock_login.return_value = ({"message": "Login successful", "access_token": "fake_token", 
                                       "user": {"client_id": 1, "username": "testuser"}}, 200)

            response = self.app.post('/auth/login', 
                                   json={'username': 'testuser', 'password': 'testpass'})
            self.assertEqual(response.status_code, 200)

            data = response.get_json()
            self.assertIn('access_token', data)
            self.assertIn('user', data)

    def test_login_endpoint_invalid_credentials(self):
        """
        Test login endpoint with invalid credentials.
        """
        with patch('app.login_user') as mock_login:
            mock_login.return_value = ({"message": "Invalid credentials"}, 401)

            response = self.app.post('/auth/login', 
                                   json={'username': 'invalid', 'password': 'invalid'})
            self.assertEqual(response.status_code, 401)

            data = response.get_json()
            self.assertIn('message', data)
            self.assertEqual(data['message'], 'Invalid credentials')

    def test_get_net_worth_with_jwt_mocked(self):
        """
        Test net worth calculation with mocked JWT functionality.
        This test bypasses actual JWT verification to focus on the business logic.
        """
        # Test the calculate_net_worth_for_user function directly with mocked JWT
        with patch('metrics.get_jwt_identity', return_value=1):
            with patch('metrics.get_db_connection') as mock_get_connection:
                # Mock database connection and cursor
                mock_connection = MagicMock()
                mock_cursor = MagicMock()
                mock_connection.cursor.return_value = mock_cursor
                mock_get_connection.return_value = mock_connection

                # Mock query result
                mock_cursor.fetchone.return_value = (250000.75,)

                # Import and test the function directly
                from metrics import calculate_net_worth_for_user
                result = calculate_net_worth_for_user()

                # Verify database interactions
                mock_get_connection.assert_called_once()
                mock_connection.cursor.assert_called_once()
                mock_cursor.execute.assert_called_once()

                # Verify result
                self.assertEqual(result, 250000.75)

                # Verify cleanup
                mock_cursor.close.assert_called_once()
                mock_connection.close.assert_called_once()

    def test_404_error_handling(self):
        """
        Test 404 error handling for non-existent endpoints.
        """
        response = self.app.get('/nonexistent')
        self.assertEqual(response.status_code, 404)

        data = response.get_json()
        self.assertIn('error', data)
        self.assertEqual(data['error'], 'Endpoint not found')

if __name__ == '__main__':
    unittest.main()