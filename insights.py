import openai
import os
import json
from flask import current_app
from flask_jwt_extended import get_jwt_identity
from database import get_db_connection, close_db_connection  # Added database import
from metrics import (
    calculate_net_worth_for_user,
    calculate_portfolio_value_for_user,
    calculate_real_estate_value_for_user,
    calculate_debt_for_user,
    calculate_equity_for_user,
    calculate_fixed_income_for_user,
    calculate_cash_for_user,
    calculate_earned_income_for_user,
    calculate_social_security_income_for_user,
    calculate_pension_income_for_user,
    calculate_real_estate_income_for_user,
    calculate_business_income_for_user,
    calculate_total_income_for_user,
    calculate_current_year_giving_for_user,
    calculate_current_year_savings_for_user,
    calculate_current_year_debt_for_user,
    calculate_current_year_taxes_for_user,
    calculate_current_year_living_expenses_for_user,
    calculate_total_expenses_for_user,
    calculate_margin_for_user,
    calculate_life_insurance_for_user,
    calculate_disability_for_user,
    calculate_ltc_for_user,
    calculate_umbrella_for_user,
    calculate_business_insurance_for_user,
    calculate_flood_insurance_for_user,
    calculate_at_risk_for_user,
    calculate_retirement_ratio_for_user,
    calculate_survivor_ratio_for_user,
    calculate_education_ratio_for_user,
    calculate_new_cars_ratio_for_user,
    calculate_ltc_ratio_for_user,
    calculate_ltd_ratio_for_user
)

def get_all_user_metrics():
    """
    Aggregate all financial metrics for the authenticated user for AI analysis.
    
    Returns:
        dict: Dictionary containing all financial metrics
    """
    try:
        # Get the client_id from the authenticated user's JWT token
        client_id = get_jwt_identity()
        
        # Aggregate all metrics
        metrics_data = {
            'client_id': client_id,
            'assets_and_liabilities': {
                'net_worth': calculate_net_worth_for_user(),
                'portfolio_value': calculate_portfolio_value_for_user(),
                'real_estate_value': calculate_real_estate_value_for_user(),
                'debt': calculate_debt_for_user(),
                'equity': calculate_equity_for_user(),
                'fixed_income': calculate_fixed_income_for_user(),
                'cash': calculate_cash_for_user()
            },
            'income_analysis': {
                'earned_income': calculate_earned_income_for_user(),
                'social_security_income': calculate_social_security_income_for_user(),
                'pension_income': calculate_pension_income_for_user(),
                'real_estate_income': calculate_real_estate_income_for_user(),
                'business_income': calculate_business_income_for_user(),
                'total_income': calculate_total_income_for_user()
            },
            'expense_tracking': {
                'current_year_giving': calculate_current_year_giving_for_user(),
                'current_year_savings': calculate_current_year_savings_for_user(),
                'current_year_debt': calculate_current_year_debt_for_user(),
                'current_year_taxes': calculate_current_year_taxes_for_user(),
                'current_year_living_expenses': calculate_current_year_living_expenses_for_user(),
                'total_expenses': calculate_total_expenses_for_user(),
                'margin': calculate_margin_for_user()
            },
            'insurance_coverage': {
                'life_insurance': calculate_life_insurance_for_user(),
                'disability': calculate_disability_for_user(),
                'ltc': calculate_ltc_for_user(),
                'umbrella': calculate_umbrella_for_user(),
                'business_insurance': calculate_business_insurance_for_user(),
                'flood_insurance': calculate_flood_insurance_for_user(),
                'at_risk': calculate_at_risk_for_user()
            },
            'future_planning_ratios': {
                'retirement_ratio': calculate_retirement_ratio_for_user(),
                'survivor_ratio': calculate_survivor_ratio_for_user(),
                'education_ratio': calculate_education_ratio_for_user(),
                'new_cars_ratio': calculate_new_cars_ratio_for_user(),
                'ltc_ratio': calculate_ltc_ratio_for_user(),
                'ltd_ratio': calculate_ltd_ratio_for_user()
            }
        }
        
        # Log the metrics data for debugging
        try:
            current_app.logger.info(f"Aggregated metrics for client {client_id}: {json.dumps(metrics_data, indent=2)}")
        except RuntimeError:
            print(f"Aggregated metrics for client {client_id}: {json.dumps(metrics_data, indent=2)}")
        
        return metrics_data
        
    except Exception as e:
        error_msg = f"Error aggregating metrics for client {client_id}: {e}"
        try:
            current_app.logger.error(error_msg)
        except RuntimeError:
            print(error_msg)
        raise Exception(error_msg)


def get_ai_configuration():
    """
    Retrieve AI configuration from the database with fallback to environment variables.
    
    Returns:
        dict: Dictionary containing base_url, api_key, and model
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Query to get AI configuration from system_config table
        query = """
            SELECT config_key, config_value
            FROM core.system_config
            WHERE config_key IN ('ai_base_url', 'ai_api_key', 'ai_model')
        """
        
        cursor.execute(query)
        config_results = cursor.fetchall()
        cursor.close()
        
        # Create a dictionary from the results
        ai_config = {}
        for config_key, config_value in config_results:
            # Remove 'ai_' prefix to match expected variable names
            if config_key == 'ai_base_url':
                ai_config['base_url'] = config_value
            elif config_key == 'ai_api_key':
                ai_config['api_key'] = config_value
            elif config_key == 'ai_model':
                ai_config['model'] = config_value
        
        # Fallback to environment variables if database values are not found or empty
        if 'base_url' not in ai_config or not ai_config['base_url']:
            ai_config['base_url'] = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        if 'api_key' not in ai_config or not ai_config['api_key']:
            ai_config['api_key'] = os.getenv('OPENAI_API_KEY', '')
        if 'model' not in ai_config or not ai_config['model']:
            ai_config['model'] = os.getenv('AI_MODEL', 'gpt-3.5-turbo')
        
        return ai_config
        
    except Exception as e:
        print(f"Error retrieving AI configuration from database: {e}")
        # Fallback to environment variables on error
        return {
            'base_url': os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'api_key': os.getenv('OPENAI_API_KEY', ''),
            'model': os.getenv('AI_MODEL', 'gpt-3.5-turbo')
        }
    finally:
        if connection:
            close_db_connection(connection)


def generate_ai_insights(metrics_data):
    """
    Generate AI insights based on financial metrics using OpenAI-compatible API.
    
    Args:
        metrics_data (dict): Dictionary containing all financial metrics
        
    Returns:
        str: AI-generated insights text
    """
    try:
        # Initialize OpenAI client with database-stored configuration
        # Fallback to environment variables if database configuration is unavailable
        import httpx
        
        ai_config = get_ai_configuration()
        base_url = ai_config['base_url']
        api_key = ai_config['api_key']
        model = ai_config['model']
        
        # Log the actual API configuration being used
        try:
            current_app.logger.info(f"AI API Configuration - Base URL: {base_url}, Model: {model}")
        except RuntimeError:
            print(f"AI API Configuration - Base URL: {base_url}, Model: {model}")
        
        # Create a custom HTTP client without proxy settings
        http_client = httpx.Client()
        
        client = openai.OpenAI(
            base_url=base_url,
            api_key=api_key,
            http_client=http_client
        )
        
        # Prepare the prompt for AI analysis
        prompt = f"""
        You are a professional financial advisor analyzing a client's comprehensive financial metrics.
        Please analyze the following financial data and provide personalized, actionable insights.

        Financial Data:
        {json.dumps(metrics_data, indent=2)}

        Please provide insights in the following exact format with these 5 sections:

        Overall Financial Health Assessment:
        [Provide 2-3 sentences about the client's overall financial health]

        Strengths in Your Financial Situation:
        - [First strength in bullet point]
        - [Second strength in bullet point]
        - [Third strength in bullet point]

        Areas Needing Improvement:
        - [First area needing improvement in bullet point]
        - [Second area needing improvement in bullet point]
        - [Third area needing improvement in bullet point]

        Specific Recommendations for Optimization:
        - [First specific recommendation in bullet point]
        - [Second specific recommendation in bullet point]
        - [Third specific recommendation in bullet point]

        Risk Considerations and Mitigation Strategies:
        - [First risk consideration in bullet point]
        - [Second risk consideration in bullet point]
        - [Third risk consideration in bullet point]

        Important formatting rules:
        - Use exactly these section headers followed by a colon
        - Use bullet points (starting with -) for all items under the sections
        - Do not use any markdown formatting like **bold**, *italic*, or # headers
        - Keep language clear, professional, and actionable
        - Focus on practical advice the client can implement
        """
        
        # Make the API call
        model_to_use = model  # Use the model from database configuration instead of environment variable
        
        # Log the API call details
        try:
            current_app.logger.info(f"Making AI API call to {base_url} with model {model_to_use}")
        except RuntimeError:
            print(f"Making AI API call to {base_url} with model {model_to_use}")
        
        response = client.chat.completions.create(
            model=model_to_use,
            messages=[
                {"role": "system", "content": "You are a professional financial advisor providing personalized insights based on comprehensive financial data. Format your response as clean, readable text without any markdown symbols or formatting characters."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=800,
            temperature=0.7
        )
        
        # Log the response details for debugging
        try:
            if hasattr(response, 'usage') and response.usage:
                current_app.logger.info(f"AI API response successful - Tokens used: {response.usage}")
            else:
                current_app.logger.info(f"AI API response successful: {response}")
        except RuntimeError:
            if hasattr(response, 'usage') and response.usage:
                print(f"AI API response successful - Tokens used: {response.usage}")
            else:
                print(f"AI API response successful: {response}")
        
        # Extract and clean the insights with better error handling
        if not response or not hasattr(response, 'choices') or len(response.choices) == 0:
            raise Exception("Invalid response from AI service: no choices returned")
        
        if not hasattr(response.choices[0], 'message') or not hasattr(response.choices[0].message, 'content'):
            raise Exception("Invalid response structure from AI service")
        
        raw_insights = response.choices[0].message.content
        
        if not raw_insights:
            raise Exception("Empty response content from AI service")
        
        # Clean up any remaining markdown formatting
        insights = clean_markdown_formatting(raw_insights)
        
        # Log the insights for debugging
        try:
            current_app.logger.info(f"Generated insights for client {metrics_data.get('client_id')}: {insights}")
        except RuntimeError:
            print(f"Generated insights for client {metrics_data.get('client_id')}: {insights}")
        
        return insights
        
    except Exception as e:
        error_msg = f"Error generating AI insights: {e}"
        try:
            current_app.logger.error(error_msg)
        except RuntimeError:
            print(error_msg)
        
        # Return a more specific error message based on the exception
        error_str = str(e).lower()
        if "invalid apikey" in error_str or "api key" in error_str:
            return "AI service authentication failed. Please contact support to resolve this issue."
        elif "openai" in str(type(e)).lower() or "api" in error_str:
            return "We're experiencing technical difficulties with our AI service. Please try again later or contact your financial advisor for a comprehensive review."
        elif "connection" in error_str:
            return "We're unable to connect to our AI service at the moment. Please check your internet connection and try again."
        else:
            return "We're unable to generate personalized insights at this moment. Please try again later or contact your financial advisor for a comprehensive review."

def clean_markdown_formatting(text):
    """
    Remove markdown formatting characters from text while preserving bullet points.
    
    Args:
        text (str): Text with potential markdown formatting
        
    Returns:
        str: Clean text without markdown formatting but with bullet points preserved
    """
    import re
    
    if not text:
        return text
    
    # Remove markdown headers (# ## ### etc.) but keep the text
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    
    # Remove bold/italic markers (**bold**, *italic*) but keep the text
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'\*(.*?)\*', r'\1', text)
    
    # Preserve bullet points by converting them to simple dash format
    # This ensures they're properly formatted for frontend parsing
    text = re.sub(r'^[\*\+]\s*', '- ', text, flags=re.MULTILINE)
    
    # Ensure numbered lists are preserved
    text = re.sub(r'^(\d+)\.\s*', r'\1. ', text, flags=re.MULTILINE)
    
    # Remove square brackets and their contents [text]
    text = re.sub(r'\[([^\]]*)\]', r'\1', text)
    
    # Remove any remaining special characters commonly used in markdown
    text = re.sub(r'[`~|>]', '', text)
    
    # Clean up extra whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)  # Normalize paragraph breaks
    text = re.sub(r' +', ' ', text)  # Replace multiple spaces with single space
    text = text.strip()  # Remove leading/trailing whitespace
    
    return text

def generate_financial_summary(metrics_data):
    """
    Generate a brief financial summary based on key metrics.
    
    Args:
        metrics_data (dict): Dictionary containing all financial metrics
        
    Returns:
        str: Brief financial summary
    """
    try:
        # Extract key metrics for summary
        net_worth = metrics_data['assets_and_liabilities']['net_worth']
        total_income = metrics_data['income_analysis']['total_income']
        total_expenses = metrics_data['expense_tracking']['total_expenses']
        margin = metrics_data['expense_tracking']['margin']
        retirement_ratio = metrics_data['future_planning_ratios']['retirement_ratio']
        
        # Create a basic summary without AI
        summary_parts = []
        
        if net_worth is not None and net_worth > 0:
            summary_parts.append(f"Your net worth is ${net_worth:,.2f}")
        
        if total_income is not None and total_income > 0:
            summary_parts.append(f"with an annual income of ${total_income:,.2f}")
        
        if total_expenses is not None and total_expenses > 0:
            summary_parts.append(f"and expenses totaling ${total_expenses:,.2f}")
        
        if margin is not None:
            if margin > 0:
                summary_parts.append(f"resulting in a positive margin of ${margin:,.2f}")
            else:
                summary_parts.append(f"resulting in a negative margin of ${abs(margin):,.2f}")
        
        if retirement_ratio is not None and retirement_ratio > 0:
            if retirement_ratio >= 1.0:
                summary_parts.append(f"Your retirement ratio of {retirement_ratio:.2f} indicates strong preparation")
            else:
                summary_parts.append(f"Your retirement ratio of {retirement_ratio:.2f} suggests room for improvement")
        
        return ". ".join(summary_parts) + "." if summary_parts else "Financial data is being processed."
        
    except Exception as e:
        error_msg = f"Error generating financial summary: {e}"
        try:
            current_app.logger.error(error_msg)
        except RuntimeError:
            print(error_msg)
        return "Unable to generate financial summary at this time."