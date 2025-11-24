# Backend - Wisdom Index Financial Advisory AI Web App

## Overview

This is the comprehensive backend implementation for the Wisdom Index Financial Advisory AI Web App. The backend provides a complete set of API endpoints for self-registration, JWT-based authentication, and extensive financial metrics calculations from a PostgreSQL database. The system supports 37+ different financial metrics across assets, liabilities, income, expenses, insurance, and future planning ratios. It also includes interactive chart data endpoints, AI-powered financial insights, and metric target management.

**Current Status**: Production-ready backend with comprehensive API endpoints. The backend has been thoroughly tested and is fully compatible with the new Next.js frontend. All 40+ API endpoints function correctly with enhanced security features and comprehensive error handling.

### Tech Stack

- **Backend Framework**: Python with Flask 2.3.3
- **Authentication**: Flask-JWT-Extended 4.5.3 for secure token-based authentication
- **Database**: PostgreSQL with existing core schema
- **Database Driver**: psycopg2 2.9.11
- **Security**: bcrypt 4.0.1 for password hashing
- **CORS**: Flask-CORS 4.0.0 for cross-origin requests
- **Environment Management**: python-dotenv 1.0.0
- **AI Integration**: OpenAI-compatible API for financial insights

## Project Structure

```
backend-beta/
├── app.py                      # Main Flask application with 40+ API endpoints
├── auth.py                     # Authentication functions and JWT utilities
├── database.py                 # Database connection and query functions
├── metrics.py                  # 37+ financial metric calculation functions and target management
├── insights.py                 # AI insights functionality
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (DB URL, JWT secrets, etc.)
├── generate_user_credentials.py # Utility script for user management
├── Documentation/              # Documentation for beta version
│   ├── specs.md               # Beta specifications
│   └── to-do.md               # Beta development tasks
├── tests/                     # Test suite for beta validation
│   └── test_auth_metrics.py   # Authentication and metrics tests
└── test_insights.py           # AI insights testing
```

## Setup Instructions

### Prerequisites

1. **Python 3.12+** installed on your system
2. **PostgreSQL** database accessible with the core schema
3. **Microsoft Visual C++ Build Tools** (for psycopg2 installation on Windows)

### Installation

1. **Navigate to the project directory**:
   ```bash
   cd backend-beta/
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   *Note: If psycopg2 fails to install due to missing build tools, you may need to install Microsoft Visual C++ Build Tools from: https://visualstudio.microsoft.com/visual-cpp-build-tools/*

3. **Configure environment variables**:
   Update the `.env` file with your database credentials and JWT secret:
   ```
   DATABASE_URL=postgresql://username:password@host:port/database_name
   JWT_SECRET_KEY=your-super-secret-jwt-key-change-this-in-production
   OPENAI_BASE_URL=
   OPENAI_API_KEY=
   AI_MODEL=
   ```

4. **Run the Flask application**:
   ```bash
   python app.py
   ```

   The API will be available at `http://localhost:5001`

## API Endpoints

### Authentication Endpoints

#### Self-Registration
**POST** `/auth/register`

Register a new user account by verifying identity against existing client records.

**Request Body**:
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john.doe@example.com",
  "username": "johndoe",
  "password": "securePassword123!"
}
```

**Response** (Success):
```json
{
  "message": "Registration successful"
}
```

**Response** (Error):
```json
{
  "message": "Client not found in our records"
}
```

#### User Login
**POST** `/auth/login`

Authenticate user and receive JWT token.

**Request Body**:
```json
{
  "username": "johndoe",
  "password": "securePassword123!"
}
```

**Response** (Success):
```json
{
  "message": "Login successful",
  "access_token": "jwt_token_string",
  "user": {
    "client_id": 1,
    "username": "johndoe",
    "email": "john.doe@example.com"
  }
}
```

**Response** (Error):
```json
{
  "message": "Invalid credentials"
}
```

#### Health Check
**GET** `/health`

Returns the service health status.

**Response**:
```json
{
  "status": "healthy",
  "service": "backend",
  "version": "1.0.0"
}
```

### Financial Metrics Endpoints

All metrics endpoints require JWT authentication via the `Authorization: Bearer <jwt_token>` header.

#### Assets & Liabilities Metrics

##### Net Worth
**GET** `/api/metrics/net-worth`

Calculates total assets minus liabilities for the authenticated user.

**Response**:
```json
{
  "metric": "net_worth",
  "value": 7868812.25,
  "user_id": 2
}
```

##### Portfolio Value
**GET** `/api/metrics/portfolio-value`

Total value of investment holdings and deposit accounts.

**Response**:
```json
{
  "metric": "portfolio_value",
  "value": 2312350.58,
  "user_id": 2
}
```

##### Real Estate Value
**GET** `/api/metrics/real-estate-value`

Total value of real estate assets.

**Response**:
```json
{
  "metric": "real_estate_value",
  "value": 2315000.00,
  "user_id": 2
}
```

##### Debt
**GET** `/api/metrics/debt`

Total outstanding debt obligations.

**Response**:
```json
{
  "metric": "debt",
  "value": 2538.33,
  "user_id": 2
}
```

##### Equity
**GET** `/api/metrics/equity`

Total value of equity investments across holdings and investment accounts.

**Response**:
```json
{
  "metric": "equity",
  "value": 1566445.70,
  "user_id": 2
}
```

##### Fixed Income
**GET** `/api/metrics/fixed-income`

Total value of fixed income investments (bonds, municipal securities, etc.).

**Response**:
```json
{
  "metric": "fixed_income",
  "value": 143495.12,
  "user_id": 2
}
```

##### Cash
**GET** `/api/metrics/cash`

Total cash and cash equivalents from holdings and investment accounts.

**Response**:
```json
{
  "metric": "cash",
  "value": 283057.85,
  "user_id": 2
}
```

#### Income Metrics

##### Earned Income
**GET** `/api/metrics/earned-income`

Income from employment and wages for the current year.

**Response**:
```json
{
  "metric": "earned_income",
  "value": 18300.00,
  "user_id": 2
}
```

##### Social Security Income
**GET** `/api/metrics/social-security-income`

Income from Social Security benefits.

**Response**:
```json
{
  "metric": "social_security_income",
  "value": 0.00,
  "user_id": 2
}
```

##### Pension Income
**GET** `/api/metrics/pension-income`

Income from pension plans.

**Response**:
```json
{
  "metric": "pension_income",
  "value": 0.0,
  "user_id": 2
}
```

##### Real Estate Income
**GET** `/api/metrics/real-estate-income`

Income from real estate investments.

**Response**:
```json
{
  "metric": "real_estate_income",
  "value": 0.00,
  "user_id": 2
}
```

##### Business Income
**GET** `/api/metrics/business-income`

Income from business operations.

**Response**:
```json
{
  "metric": "business_income",
  "value": 0.00,
  "user_id": 2
}
```

##### Total Income
**GET** `/api/metrics/total-income`

Sum of all income sources for the current year.

**Response**:
```json
{
  "metric": "total_income",
  "value": 18300.00,
  "user_id": 2
}
```

#### Expense Metrics

##### Current Year Giving
**GET** `/api/metrics/current-year-giving`

Charitable contributions for the current year.

**Response**:
```json
{
  "metric": "current_year_giving",
  "value": 85000.00,
  "user_id": 2
}
```

##### Current Year Savings
**GET** `/api/metrics/current-year-savings`

Active savings contributions for the current year.

**Response**:
```json
{
  "metric": "current_year_savings",
  "value": 11100.00,
 "user_id": 2
}
```

##### Current Year Debt
**GET** `/api/metrics/current-year-debt`

Annual debt payments for active loans in the current year.

**Response**:
```json
{
  "metric": "current_year_debt",
  "value": 211.53,
 "user_id": 2
}
```

##### Current Year Taxes
**GET** `/api/metrics/current-year-taxes`

Estimated tax obligations for the current year (calculated as 15% of income).

**Response**:
```json
{
  "metric": "current_year_taxes",
  "value": 27450.00,
  "user_id": 2
}
```

##### Current Year Living Expenses
**GET** `/api/metrics/current-year-living-expenses`

Living expenses for the current year.

**Response**:
```json
{
  "metric": "current_year_living_expenses",
  "value": 165000.00,
  "user_id": 2
}
```

##### Total Expenses
**GET** `/api/metrics/total-expenses`

Sum of all expense categories for the current year.

**Response**:
```json
{
  "metric": "total_expenses",
  "value": 388661.53,
  "user_id": 2
}
```

##### Margin
**GET** `/api/metrics/margin`

Difference between total income and total expenses.

**Response**:
```json
{
  "metric": "margin",
  "value": -205661.53,
 "user_id": 2
}
```

#### Insurance Metrics

##### Life Insurance
**GET** `/api/metrics/life-insurance`

Total life insurance death benefit coverage.

**Response**:
```json
{
  "metric": "life_insurance",
  "value": 3700000.00,
  "user_id": 2
}
```

##### Disability Insurance
**GET** `/api/metrics/disability`

Disability insurance coverage amount.

**Response**:
```json
{
  "metric": "disability",
  "value": 13000.00,
  "user_id": 2
}
```

##### Long-Term Care Insurance
**GET** `/api/metrics/ltc`

Long-term care insurance coverage.

**Response**:
```json
{
  "metric": "ltc",
  "value": 12000.00,
  "user_id": 2
}
```

##### Umbrella Insurance
**GET** `/api/metrics/umbrella`

Umbrella insurance coverage amount.

**Response**:
```json
{
  "metric": "umbrella",
  "value": 10000.00,
  "user_id": 2
}
```

##### Business Insurance
**GET** `/api/metrics/business-insurance`

Business insurance coverage.

**Response**:
```json
{
  "metric": "business_insurance",
  "value": 1000.00,
  "user_id": 2
}
```

##### Flood Insurance
**GET** `/api/metrics/flood-insurance`

Flood insurance coverage.

**Response**:
```json
{
  "metric": "flood_insurance",
  "value": 0.00,
  "user_id": 2
}
```

##### At Risk
**GET** `/api/metrics/at-risk`

Assets at risk without adequate insurance coverage.

**Response**:
```json
{
  "metric": "at_risk",
  "value": 378435.19,
  "user_id": 2
}
```

#### Future Planning Ratios

##### Retirement Ratio
**GET** `/api/metrics/retirement-ratio`

Ratio of retirement resources to retirement needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "retirement_ratio",
  "value": 0.51,
  "user_id": 2
}
```

##### Survivor Ratio
**GET** `/api/metrics/survivor-ratio`

Ratio of survivor resources to survivor needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "survivor_ratio",
  "value": 0.64,
  "user_id": 2
}
```

##### Education Ratio
**GET** `/api/metrics/education-ratio`

Ratio of education resources to education needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "education_ratio",
  "value": 0.28,
  "user_id": 2
}
```

##### New Cars Ratio
**GET** `/api/metrics/new-cars-ratio`

Ratio of car replacement resources to needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "new_cars_ratio",
  "value": 0.91,
  "user_id": 2
}
```

##### LTC Ratio
**GET** `/api/metrics/ltc-ratio`

Ratio of long-term care resources to needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "ltc_ratio",
  "value": 0.44,
  "user_id": 2
}
```

##### LTD Ratio
**GET** `/api/metrics/ltd-ratio`

Ratio of long-term disability resources to needs (target: ≥1.0).

**Response**:
```json
{
  "metric": "ltd_ratio",
  "value": 0.07,
  "user_id": 2
}
```

### Chart Data Endpoints

All chart endpoints require JWT authentication via the `Authorization: Bearer <jwt_token>` header.

##### Income Breakdown Bar Chart
**GET** `/api/charts/income-bar-chart`

Returns income data for bar chart visualization.

**Response**:
```json
{
  "chart_type": "income_bar_chart",
  "data": [
    {"income_category": "Earned Income", "amount": 183000.00},
    {"income_category": "Social Security", "amount": 0.00},
    {"income_category": "Pension", "amount": 0.0},
    {"income_category": "Real Estate", "amount": 0.0},
    {"income_category": "Business", "amount": 0.00}
  ],
 "user_id": 2
}
```

##### Expense Distribution Pie Chart
**GET** `/api/charts/expense-pie-chart`

Returns expense data for pie chart visualization.

**Response**:
```json
{
  "chart_type": "expense_pie_chart",
  "data": [
    {"expense_category": "Giving", "amount": 85000.00},
    {"expense_category": "Savings", "amount": 111000.00},
    {"expense_category": "Debt", "amount": 211.53},
    {"expense_category": "Taxes", "amount": 27450.00},
    {"expense_category": "Living", "amount": 165000.00}
  ],
  "user_id": 2
}
```

### Client Profile Endpoint

All client profile endpoints require JWT authentication via the `Authorization: Bearer <jwt_token>` header.

##### Get Client Profile
**GET** `/api/client-profile`

Returns detailed client profile information for authenticated user.

**Response**:
```json
{
  "client_profile": {
    "first_name": "John",
    "last_name": "Doe",
    "email": "john.doe@example.com",
    "date_of_birth": "1970-01-01",
    "address": "123 Main St, Anytown, ST 12345",
    "phone": "+1-555-123-4567"
  },
  "user_id": 2
}
```

### Metric Target Management Endpoints

All target management endpoints require JWT authentication via the `Authorization: Bearer <jwt_token>` header.

##### Set Metric Target
**POST** `/api/targets`

Set a target value for a specific metric for the authenticated user.

**Request Body**:
```json
{
  "metric_name": "net_worth",
  "target_value": 10000.00,
  "category": "assets_liabilities"
}
```

**Response** (Success):
```json
{
  "message": "Target set successfully",
  "result": true
}
```

**Response** (Error):
```json
{
  "error": "Internal server error"
}
```

##### Get All Metric Targets
**GET** `/api/targets`

Get all metric targets for the authenticated user.

**Response** (Success):
```json
{
  "targets": [
    {"metric_name": "net_worth", "target_value": 1000.00, "category": "assets_liabilities"},
    {"metric_name": "retirement_ratio", "target_value": 1.0, "category": "future_planning"}
  ]
}
```

**Response** (Error):
```json
{
  "error": "Internal server error"
}
```

##### Delete Metric Target
**DELETE** `/api/targets/{metric_name}`

Delete a specific metric target for the authenticated user.

**Response** (Success):
```json
{
  "message": "Target deleted successfully",
  "result": true
}
```

**Response** (Error):
```json
{
  "error": "Internal server error"
}
```

### AI Insights Endpoint

All insights endpoints require JWT authentication via the `Authorization: Bearer <jwt_token>` header.

##### Generate AI Insights
**POST** `/api/insights/generate`

Generates personalized financial insights based on all user metrics.

**Request Body** (Optional):
```json
{
 "include_summary": true
}
```

**Response**:
```json
{
  "insights": "Overall Financial Health Assessment:\n[Assessment text]\n\nStrengths in Your Financial Situation:\n- [Strength 1]\n- [Strength 2]\n- [Strength 3]\n\nAreas Needing Improvement:\n- [Improvement 1]\n- [Improvement 2]\n- [Improvement 3]\n\nSpecific Recommendations for Optimization:\n- [Recommendation 1]\n- [Recommendation 2]\n- [Recommendation 3]\n\nRisk Considerations and Mitigation Strategies:\n- [Risk 1]\n- [Risk 2]\n- [Risk 3]",
  "user_id": 2,
  "timestamp": "2025-10-30T06:30:00Z"
}
```

## Error Codes

- `200`: Success
- `400`: Bad request (validation errors)
- `401`: Unauthorized (missing or invalid JWT token)
- `404`: Endpoint not found
- `500`: Internal server error (database issues)

## Testing

### Manual Testing

Use curl or Postman to test the API endpoints:

```bash
# Health check
curl http://localhost:5001/health

# Register a new user (must exist in core.clients table)
curl -X POST -H "Content-Type: application/json" \
  -d '{"first_name":"John","last_name":"Doe","email":"john.doe@example.com","username":"johndoe","password":"securePassword123!"}' \
  http://localhost:5001/auth/register

# Login to get JWT token
curl -X POST -H "Content-Type: application/json" \
 -d '{"username":"johndoe","password":"securePassword123!"}' \
  http://localhost:5001/auth/login

# Use the returned token to access metrics (replace <token> with actual token)
curl -H "Authorization: Bearer <token>" \
  http://localhost:5001/api/metrics/net-worth
```

### Automated Testing

Run the test suite:

```bash
# Run all tests
python -m unittest tests/test_auth_metrics.py

# Run with verbose output
python -m unittest -v tests/test_auth_metrics.py

# Test AI insights
python test_insights.py
```

### Test Coverage

The test suite includes:
- ✅ Health check endpoint
- ✅ Authentication required for all metrics endpoints
- ✅ Login endpoint with valid credentials
- ✅ Login endpoint with invalid credentials
- ✅ Registration endpoint with valid client information
- ✅ Registration endpoint with invalid client information
- ✅ Missing credentials in login
- ✅ 404 error handling
- ✅ All 37+ metrics endpoints functionality
- ✅ Chart data endpoints
- ✅ Client profile endpoint
- ✅ Target management endpoints
- ✅ AI insights generation

## Security Features

### Self-Registration with Client Verification

Clients can register themselves by providing their first and last name, which is verified against the core.clients table before account creation.

### Password Security

Passwords are securely hashed using bcrypt before storage in the database.

### JWT Authentication

All metric endpoints require a valid JWT token in the Authorization header, ensuring only authenticated users can access their data.

### Parameterized Queries

All database queries use parameterized statements to prevent SQL injection.

### Data Isolation

Each authenticated user only receives data specific to their client_id extracted from the JWT token, ensuring proper data isolation between users.

### Input Validation

All inputs are validated for type and range before processing.

## Database Schema Requirements

The backend expects the following tables in the `core` schema:

### Core Tables
- `core.clients` (client_id, first_name, last_name, email, hh_date_of_birth, and other client information)
- `core.users` (user_id, client_id, username, email, password_hash, created_at, last_login)
- `core.metric_targets` (client_id, metric_name, target_value, category, created_at, updated_at)

### Asset Tables
- `core.holdings` (client_id, value, asset_class)
- `core.real_estate_assets` (client_id, total_value)
- `core.businesses` (client_id, amount)
- `core.investment_deposit_accounts` (client_id, total_value, holdings_value, fact_type_name, sub_type, cash_balance)
- `core.personal_property_accounts` (client_id, total_value)

### Liability Tables
- `core.liability_note_accounts` (client_id, total_value, interest_rate, loan_term_in_years, payment_frequency, loan_date, repayment_type)

### Income Tables
- `core.incomes` (client_id, income_type, current_year_amount, annual_amount, end_type, end_value, deleted)

### Expense Tables
- `core.expenses` (client_id, type, sub_type, expense_item, annual_amount, start_actual_date, end_actual_date)

### Savings Tables
- `core.savings` (client_id, calculated_annual_amount_usd, fixed_amount_usd, destination, account_id, start_type)

### Insurance Tables
- `core.life_insurance_annuity_accounts` (client_id, fact_type_name, death_benefit)
- `core.disability_ltc_insurance_accounts` (client_id, fact_type_name, sub_type, benefit_amount, annual_premium)
- `core.property_casualty_insurance_accounts` (client_id, sub_type, maximum_annual_benefit)

## Advanced Features

### Comprehensive Metrics System

The backend implements 37+ financial metrics across 5 categories:

1. **Assets & Liabilities (7 metrics)**: Net Worth, Portfolio Value, Real Estate Value, Debt, Equity, Fixed Income, Cash
2. **Income (6 metrics)**: Earned Income, Social Security Income, Pension Income, Real Estate Income, Business Income, Total Income
3. **Expenses (7 metrics)**: Giving, Savings, Debt Payments, Taxes, Living Expenses, Total Expenses, Margin
4. **Insurance (8 metrics)**: Life Insurance, Disability, LTC, Umbrella, Business Insurance, Flood Insurance, At Risk
5. **Future Planning Ratios (6 metrics)**: Retirement Ratio, Survivor Ratio, Education Ratio, New Cars Ratio, LTC Ratio, LTD Ratio

### Interactive Chart Data

The backend provides chart data for:
- Income breakdown bar chart (5 categories)
- Expense distribution pie chart (5 categories)

### Client Profile Information

The backend provides detailed client profile information for authenticated users.

### Metric Target Management

The backend includes comprehensive target management functionality that allows users to set, retrieve, update, and delete financial targets for metrics with comparison logic for visual indicators.

### AI-Powered Insights

The backend includes AI insights functionality that:
- Aggregates all 37+ user metrics for comprehensive analysis
- Generates personalized financial recommendations
- Provides risk considerations and optimization strategies
- Integrates with OpenAI-compatible API service

### Sophisticated Ratio Calculations

The ratio metrics use complex present value calculations with:
- 20-year planning horizons for long-term scenarios
- 4% discount rate for present value calculations
- Age-based retirement planning (retirement at age 65)
- Dynamic expense and income projections
- Insurance coverage gap analysis

### Debug Logging

The system includes comprehensive debug logging for:
- SQL query parameter validation
- Query execution monitoring
- Result processing validation
- Error tracking and reporting

### Error Handling

Robust error handling includes:
- Graceful handling of null/missing data
- Database connection error recovery
- Invalid input validation
- SQL injection prevention
- Comprehensive HTTP status codes

## Development Notes

### Current Implementation Status

✅ **Fully Implemented Features**:
1. **Self-Registration**: Clients can register by verifying their identity against existing client records
2. **JWT Authentication**: Secure token-based authentication system with 1-hour expiration
3. **Complete Metrics Suite**: All 37+ financial metrics with authenticated user context
4. **Data Isolation**: Users can only access their own data based on client_id
5. **Password Security**: Passwords are hashed using bcrypt
6. **Comprehensive Error Handling**: Proper error responses for all failure scenarios
7. **Advanced Ratio Calculations**: Sophisticated financial planning ratios with present value calculations
8. **Interactive Chart Data**: Income and expense visualization endpoints
10. **Client Profile**: Detailed client profile information retrieval
11. **Metric Target Management**: Complete CRUD operations for metric targets with comparison logic
12. **AI Insights**: Personalized financial recommendations using AI
13. **Debug Logging**: Comprehensive logging for development and troubleshooting
14. **Export Functionality**: Data export capabilities for client-side processing

### Performance Optimizations

- **Batch Processing**: Metrics can be loaded in batches for better frontend performance
- **Parameterized Queries**: All SQL queries use parameterization for security and performance
- **Connection Management**: Proper database connection handling with cleanup
- **Efficient Calculations**: Optimized SQL queries with CTEs and joins for complex calculations

### Production Readiness

The backend is production-ready with:
- Secure authentication system
- Comprehensive error handling
- Input validation and sanitization
- Database connection management
- CORS support for frontend integration
- Environment-based configuration
- Health check endpoint for monitoring
- Target management with data persistence
- Client profile information retrieval
- Export functionality for client-side data processing

### Next.js Frontend Compatibility

The backend is fully compatible with the new Next.js frontend and provides:
- CORS configuration for cross-origin requests from Next.js development server
- JWT authentication that works seamlessly with Next.js client-side token management
- All endpoints optimized for Next.js data fetching patterns
- Error responses formatted for Next.js error boundary handling

## Troubleshooting

### Common Issues

1. **psycopg2 installation fails**:
   - Install Microsoft Visual C++ Build Tools
   - Or use: `pip install psycopg2-binary --only-binary=all`

2. **Database connection errors**:
   - Verify DATABASE_URL in .env file
   - Ensure PostgreSQL is running and accessible
   - Check database credentials and permissions

3. **Authentication fails**:
   - Verify JWT_SECRET_KEY is set in .env
   - Ensure correct Authorization header format: `Bearer <token>`

4. **Registration fails**:
   - Verify the client exists in the core.clients table with matching first and last name
   - Ensure all required fields are provided

5. **Metrics return null values**:
   - Check if client has data in the respective tables
   - Verify client_id mapping between users and clients table
   - Review debug logs for query execution details

6. **AI Insights not generating**:
   - Verify OpenAI API credentials in .env
   - Check API endpoint accessibility
   - Review error logs for specific issues

7. **Target Management Issues**:
   - Ensure `core.metric_targets` table exists in database
   - Check that client_id is properly extracted from JWT token
   - Verify target CRUD operations in logs

### Debug Information

The application provides extensive debug logging including:
- SQL query parameter counts and validation
- Query execution details with client IDs
- Result processing information
- Error details with context
- Target management operation logs
- Client profile retrieval logs
- Export functionality logs

Enable debug logging by checking the console output when running the application.

## License

This project is part of the Wisdom Index Financial Advisory AI Web App development.

---

**Version**: 1.0.0 (Production Ready)
**Last Updated**: November 2025
**Status**: Fully Implemented with 37+ Financial Metrics, Chart Data, AI Insights, Client Profile, and Target Management
**Frontend Compatibility**: Fully compatible with Next.js frontend (port 3000)
**Recent Changes**: Backend remains stable while frontend has been migrated to Next.js with TypeScript
**API Endpoints**: 40+ endpoints fully functional including authentication, metrics, charts, insights, targets, and client profile