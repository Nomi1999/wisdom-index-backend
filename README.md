# Wisdom Index Financial Advisory AI Web App - Backend

## Overview

The backend of the Wisdom Index Financial Advisory AI Web App is a comprehensive financial dashboard platform built with Python Flask. It provides secure API endpoints for fetching financial metrics, user authentication, AI-powered insights, and administrative functions. The backend uses PostgreSQL for data storage and JWT-based authentication to ensure secure access to financial data.

## Features

- **37+ Financial Metrics**: Comprehensive financial calculations across 6 key categories (Assets & Liabilities, Income Analysis, Expense Tracking, Insurance Coverage, Future Planning Ratios, and Wisdom Index Ratios)
- **AI-Powered Insights**: One-click generation of personalized financial insights using OpenAI-compatible API with streaming responses
- **Secure Authentication**: JWT-based authentication with role-based access control (client and admin)
- **Administrative Dashboard**: Complete administrative tools for client management, analytics, and database access
- **Data Export**: Excel export functionality for all financial metrics and chart data
- **Interactive Visualizations**: API endpoints for income bar charts, expense pie charts, treemap visualizations, and financial ratio charts
- **Target Management**: Ability to set financial targets for each metric
- **Account History**: Historical financial data tracking with interactive visualizations

## Tech Stack

### Backend
- **Framework**: Python Flask 2.3.3 with JWT authentication
- **Authentication**: Flask-JWT-Extended 4.5.3 for secure user authentication
- **Database**: PostgreSQL with comprehensive financial data schema
- **Database Driver**: psycopg2 2.9.11 for secure database connections
- **Security**: bcrypt 4.0.1 for secure password hashing
- **AI Integration**: OpenAI SDK 1.30.1 for AI service integration with configurable base URL
- **Data Export**: openpyxl 3.1.2 for Excel export functionality
- **WSGI Server**: Waitress 2.1.2 for production deployment
- **CORS**: Flask-CORS 4.0.0 for cross-origin resource sharing configuration
- **Environment Management**: python-dotenv 1.0 for environment variables like DB credentials

### Architecture
- **Microservices-like architecture**: Clear separation of concerns between frontend and backend
- **JWT Token-Based Authentication**: Secure token-based authentication system
- **Role-Based Access Control**: Separate client and admin access levels
- **Parameterized Queries**: Secure database queries to prevent SQL injection

## API Endpoints

### Authentication Endpoints
- `POST /auth/login` - Authenticate user and return JWT token
- `POST /auth/register` - Register a new user if they exist in the core.clients table
- `POST /api/auth/admin/register` - Register a new admin user with security code validation
- `GET /auth/verify` - Verify JWT token and return user information

### Metrics Endpoints (37+ endpoints)
- `GET /api/metrics/net-worth` - Net Worth metric with target comparison
- `GET /api/metrics/portfolio-value` - Portfolio Value metric
- `GET /api/metrics/real-estate-value` - Real Estate Value metric
- `GET /api/metrics/debt` - Debt metric
- `GET /api/metrics/equity` - Equity metric
- `GET /api/metrics/fixed-income` - Fixed Income metric
- `GET /api/metrics/cash` - Cash metric
- `GET /api/metrics/earned-income` - Earned Income metric
- `GET /api/metrics/social-security-income` - Social Security Income metric
- `GET /api/metrics/pension-income` - Pension Income metric
- `GET /api/metrics/real-estate-income` - Real Estate Income metric
- `GET /api/metrics/business-income` - Business Income metric
- `GET /api/metrics/total-income` - Total Income metric
- `GET /api/metrics/current-year-giving` - Current Year Giving metric
- `GET /api/metrics/current-year-savings` - Current Year Savings metric
- `GET /api/metrics/current-year-debt` - Current Year Debt metric
- `GET /api/metrics/current-year-taxes` - Current Year Taxes metric
- `GET /api/metrics/current-year-living-expenses` - Current Year Living Expenses metric
- `GET /api/metrics/total-expenses` - Total Expenses metric
- `GET /api/metrics/margin` - Margin metric
- `GET /api/metrics/life-insurance` - Life Insurance metric
- `GET /api/metrics/disability` - Disability metric
- `GET /api/metrics/ltc` - LTC (Long Term Care) metric
- `GET /api/metrics/umbrella` - Umbrella metric
- `GET /api/metrics/business-insurance` - Business Insurance metric
- `GET /api/metrics/flood-insurance` - Flood Insurance metric
- `GET /api/metrics/at-risk` - At Risk metric
- `GET /api/metrics/retirement-ratio` - Retirement Ratio metric
- `GET /api/metrics/survivor-ratio` - Survivor Ratio metric
- `GET /api/metrics/education-ratio` - Education Ratio metric
- `GET /api/metrics/new-cars-ratio` - New Cars Ratio metric
- `GET /api/metrics/ltc-ratio` - LTC Ratio metric
- `GET /api/metrics/ltd-ratio` - LTD Ratio metric
- `GET /api/metrics/savings-ratio` - Savings Ratio metric
- `GET /api/metrics/giving-ratio` - Giving Ratio metric
- `GET /api/metrics/reserves-ratio` - Reserves Ratio metric
- `GET /api/metrics/debt-ratio` - Debt Ratio metric
- `GET /api/metrics/diversification-ratio` - Diversification Ratio metric

### Chart Visualization Endpoints (12 endpoints)
- `GET /api/charts/income-bar-chart` - Income data for bar chart visualization
- `GET /api/charts/expense-pie-chart` - Expense data for pie chart visualization
- `GET /api/charts/treemap` - Treemap data for visualization
- `GET /api/charts/bar-chart` - Bar chart data for financial ratios
- `GET /api/admin/client/{client_id}/charts/income-bar-chart` - Admin: Income chart for specific client
- `GET /api/admin/client/{client_id}/charts/expense-pie-chart` - Admin: Expense chart for specific client
- `GET /api/admin/client/{client_id}/charts/treemap` - Admin: Treemap data for specific client
- `GET /api/admin/client/{client_id}/charts/bar-chart` - Admin: Bar chart for specific client
- `GET /api/admin/clients/compare/charts/treemap` - Compare treemap data for two clients
- `GET /api/admin/clients/compare/charts/bar-chart` - Compare bar chart data for two clients
- `GET /api/admin/clients/compare/charts/income-bar-chart` - Compare income chart data for two clients
- `GET /api/admin/clients/compare/charts/expense-pie-chart` - Compare expense chart data for two clients

### Profile Management Endpoints
- `GET /api/profile` - Fetch client profile information
- `PUT /api/profile` - Update client profile information
- `GET /api/client-name` - Fetch client name for the authenticated user

### AI Insights Endpoints
- `POST /api/insights/generate` - Generate AI-powered financial insights
- `POST /api/admin/client/{client_id}/insights/generate` - Generate AI insights for a specific client (admin access)

### Data Export Endpoints
- `GET /api/export-data` - Export all financial metrics and chart data as Excel file

### Target Management Endpoints (5 endpoints)
- `GET /api/targets` - Get all target values for the authenticated user
- `POST /api/targets` - Update multiple target values for the authenticated user
- `PUT /api/targets/{metric_name}` - Update a single target value for the authenticated user
- `DELETE /api/targets/{metric_name}` - Delete the most recent target value for a specific metric
- `DELETE /api/targets` - Delete all target values for the authenticated user

### Metric Detail Modal Endpoints (3 endpoints)
- `GET /api/metrics/{metric_name}/details` - Get metric details including formula, description, and tables used
- `GET /api/data/{table_name}` - Get raw table data for authenticated user
- `GET /api/tables/{metric_name}` - Get list of tables used by a specific metric

### Admin Endpoints (23+ endpoints)
- `GET /api/admin/clients` - Get all clients for admin dashboard
- `GET /api/admin/client/{client_id}/metrics` - Get all metrics for a specific client (admin access)
- `GET /api/admin/tables/{table_name}` - Get data from a specific database table
- `GET /api/admin/analytics` - Get aggregate analytics across all clients
- `GET /api/admin/client/{client_id}/targets` - Get all targets for a specific client (admin access)
- `POST /api/admin/client/{client_id}/targets` - Update targets for a specific client (admin access)
- `DELETE /api/admin/client/{client_id}/targets/{metric_name}` - Delete a specific target for a client (admin access)
- `DELETE /api/admin/client/{client_id}/targets` - Delete all targets for a specific client (admin access)
- `GET /api/admin/clients-summary` - Get all clients with key metrics summary for admin dashboard
- `GET /api/admin/security-code` - Get current security code status and last updated information
- `PUT /api/admin/security-code` - Update the admin security code
- `POST /api/admin/security-code/validate` - Validate a security code without changing it
- `GET /api/admin/users` - Get all admin users for user permissions management
- `PUT /api/admin/users/{user_id}/role` - Update admin user role (superuser status)
- `DELETE /api/admin/users/{user_id}` - Delete an admin user record
- `GET /api/admin/activity-logs` - Get user activity logs for monitoring
- `GET /api/admin/permissions/policies` - Get current access control policies
- `PUT /api/admin/permissions/policies/{policy_name}` - Update an access control policy
- `GET /api/admin/ai-config` - Get current AI configuration values (superuser only)
- `PUT /api/admin/ai-config` - Update AI configuration values (superuser only)

### Account History Endpoints (4 endpoints)
- `GET /api/accounts` - Get all accounts for the authenticated user
- `GET /api/accounts/{account_id}/history` - Get historical data for a specific account
- `POST /api/accounts/history` - Get historical data for multiple accounts
- `GET /api/accounts/{account_id}/summary` - Get summary statistics for a specific account
- `GET /api/admin/client/{client_id}/accounts` - Get all accounts for a specific client (admin access)
- `GET /api/admin/client/{client_id}/accounts/{account_id}/history` - Get historical data for a specific account of a specific client (admin access)

### Health Check Endpoint
- `GET /health` - Health check endpoint to verify the API is running

## Environment Variables

Create a `.env` file in the backend-beta directory with the following variables:

```env
# Database Configuration
DATABASE_URL=postgresql://username:password@hostname:port/database_name

# JWT Configuration
JWT_SECRET_KEY=your-super-secret-jwt-key-change-this-in-production

# Environment
ENVIRONMENT=development
RAILWAY_ENVIRONMENT=

# CORS Configuration (for production)
FRONTEND_URL=https://your-vercel-domain.vercel.app

# AI Configuration (optional)
AI_BASE_URL=https://api.openai.com/v1
AI_MODEL=gpt-3.5-turbo
AI_API_KEY=your-openai-api-key

# Admin Security Code
ADMIN_SECURITY_CODE=your-admin-security-code
```

## Setup Instructions

### Local Development

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd backend-beta
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file based on `.env.example` and configure your environment variables

5. Start the server:
   ```bash
   python app.py
   ```
   
   The server will run on `http://localhost:5001` by default

### Production Deployment

The backend is configured for deployment to Railway using the waitress WSGI server:

1. The application uses the `Procfile` and `railway.json` for Railway deployment
2. Set the environment variables in your Railway dashboard
3. The application is configured to use waitress as the WSGI server
4. Health checks are performed on the `/health` endpoint

## Database Schema

The backend expects the following tables in the `core` schema:

- `core.clients` (client information)
- `core.users` (user authentication and roles)
- `core.holdings` (investment holdings)
- `core.real_estate_assets` (real estate assets)
- `core.businesses` (business assets)
- `core.investment_deposit_accounts` (investment accounts)
- `core.personal_property_accounts` (personal property)
- `core.liability_note_accounts` (liabilities and debts)
- `core.incomes` (income streams)
- `core.expenses` (expenses)
- `core.savings` (savings plans)
- `core.life_insurance_annuity_accounts` (life insurance)
- `core.disability_ltc_insurance_accounts` (disability and LTC insurance)
- `core.property_casualty_insurance_accounts` (property insurance)
- `core.metric_targets` (user-defined targets for metrics)
- `core.charities` (charitable contributions)
- `core.entity_interests` (entity interests)
- `core.account_history` (historical account data)
- `core.facts` (general facts)
- `core.flows` (financial flows)
- `core.values` (values)
- `core.vw_expense_summary` (expense summary view)
- `core.system_config` (system configuration including AI settings)

## Security

- **JWT Authentication**: Secure token-based authentication system
- **Role-Based Access**: Separate client and admin access levels
- **Session Management**: Robust session validation and security measures
- **Superuser Protection**: Enhanced security for sensitive administrative functions
- **Database Security**: Protected database access with role-based restrictions
- **Password Security**: bcrypt-based password hashing
- **Parameterized Queries**: All database queries use parameterization to prevent SQL injection

## AI Integration

The backend supports OpenAI-compatible API integration with configurable base URL, allowing for:

- AI-powered financial insights generation
- Configurable AI model and service provider
- Streaming responses for real-time insights
- Five-section format for structured insights (overall financial health, strengths, areas for improvement, specific recommendations, and risk considerations)

## Error Handling

The backend includes comprehensive error handling for:

- Authentication failures
- Database connection issues
- Invalid API requests
- Missing or malformed data
- Rate limiting (to be implemented)

## Deployment

The application is designed to be deployed on Railway with the following configuration:

- Uses waitress as the WSGI server for production
- Listens on the port provided by Railway ($PORT)
- Serves on 0.0.0.0 for Railway's infrastructure
- Uses the /health endpoint for health checks
- Automatically configures CORS based on environment variables

## Troubleshooting

1. Check that all environment variables are properly set
2. Verify database connection string is correct
3. Ensure frontend URL is properly configured for CORS
4. Check that the JWT secret key is set and matches the frontend
5. Verify the AI API key is valid if using AI features
6. Check logs for specific error messages


