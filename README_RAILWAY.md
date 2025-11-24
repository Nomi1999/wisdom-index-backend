# Wisdom Index Backend - Railway Deployment

This backend is configured for deployment to Railway using the waitress WSGI server.

## Deployment Files

- `Procfile`: Tells Railway how to run the application
- `railway.json`: Railway-specific configuration
- `.env.example`: Example environment variables

## Environment Variables

Set these in your Railway dashboard:

### Required
- `DATABASE_URL`: PostgreSQL connection string
- `JWT_SECRET_KEY`: Secret key for JWT tokens
- `ENVIRONMENT`: Set to "production"

### Optional but Recommended
- `FRONTEND_URL`: Your Vercel frontend URL (for CORS)
- `AI_BASE_URL`: AI service base URL
- `AI_MODEL`: AI model identifier
- `AI_API_KEY`: AI service API key
- `ADMIN_SECURITY_CODE`: Security code for admin registration

## Railway Configuration

The application is configured to:
- Use waitress as the WSGI server
- Listen on the port provided by Railway ($PORT)
- Serve on 0.0.0.0 for Railway's infrastructure
- Use the /health endpoint for health checks

## Database Configuration

Ensure your PostgreSQL database:
- Uses SSL connections (required by the app)
- Is accessible from Railway's IP addresses
- Has the proper schema (core) and tables

## CORS Configuration

The backend automatically configures CORS based on the environment:
- Production: Allows requests from your FRONTEND_URL
- Development: Allows requests from localhost:3000

## Troubleshooting

1. Check Railway build logs for any errors
2. Verify all environment variables are set
3. Ensure database connection string is correct
4. Check that the frontend URL is properly configured for CORS