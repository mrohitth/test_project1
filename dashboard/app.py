"""
Analytics Dashboard Application
- Read-only database access
- No ETL operations
- Environment variable configuration
- Graceful error handling
"""

import os
import logging
from typing import Optional, Dict, Any
import pandas as pd
from flask import Flask, render_template, jsonify, request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'analytics_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_DRIVER = os.getenv('DB_DRIVER', 'postgresql')

# Build database connection string
if DB_PASSWORD:
    DATABASE_URL = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = f"{DB_DRIVER}://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize database engine with read-only connection
engine = None


def init_db_connection():
    """Initialize database connection with error handling."""
    global engine
    try:
        engine = create_engine(
            DATABASE_URL,
            echo=os.getenv('DEBUG_SQL', 'False').lower() == 'true',
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to establish database connection: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database initialization: {str(e)}")
        return False


def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
    """
    Execute a read-only SQL query safely.
    
    Args:
        query: SQL query to execute
        params: Optional query parameters
        
    Returns:
        DataFrame with results or None if error occurs
    """
    try:
        if engine is None:
            logger.error("Database engine not initialized")
            return None
            
        if not is_select_query(query):
            logger.warning(f"Non-SELECT query rejected: {query[:50]}...")
            return None
            
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
    except SQLAlchemyError as e:
        logger.error(f"Database query error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {str(e)}")
        return None


def is_select_query(query: str) -> bool:
    """
    Validate that query is read-only (SELECT only).
    
    Args:
        query: SQL query to validate
        
    Returns:
        True if query is SELECT, False otherwise
    """
    query_upper = query.strip().upper()
    
    # Block write operations
    blocked_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE']
    for keyword in blocked_keywords:
        if query_upper.startswith(keyword):
            return False
    
    # Only allow SELECT
    return query_upper.startswith('SELECT')


# Routes
@app.route('/', methods=['GET'])
def index():
    """Dashboard homepage."""
    try:
        return render_template('dashboard.html')
    except Exception as e:
        logger.error(f"Error loading dashboard: {str(e)}")
        return jsonify({'error': 'Failed to load dashboard'}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        if engine is None:
            return jsonify({
                'status': 'unhealthy',
                'message': 'Database not initialized'
            }), 503
            
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': pd.Timestamp.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'message': str(e)
        }), 503


@app.route('/api/analytics/summary', methods=['GET'])
def get_analytics_summary():
    """Get analytics summary data (read-only)."""
    try:
        query = """
        SELECT 
            COUNT(*) as total_records,
            MAX(created_at) as latest_record
        FROM analytics_data
        """
        df = execute_query(query)
        
        if df is None:
            return jsonify({'error': 'Failed to retrieve analytics summary'}), 500
            
        return jsonify(df.to_dict(orient='records')[0]), 200
    except Exception as e:
        logger.error(f"Error getting analytics summary: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/analytics/metrics', methods=['GET'])
def get_metrics():
    """Get metrics data with optional filters (read-only)."""
    try:
        # Get filter parameters from query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        query = "SELECT * FROM analytics_metrics WHERE 1=1"
        params = {}
        
        if start_date:
            query += " AND date >= :start_date"
            params['start_date'] = start_date
            
        if end_date:
            query += " AND date <= :end_date"
            params['end_date'] = end_date
            
        query += " ORDER BY date DESC LIMIT 1000"
        
        df = execute_query(query, params)
        
        if df is None:
            return jsonify({'error': 'Failed to retrieve metrics'}), 500
            
        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/analytics/custom', methods=['POST'])
def execute_custom_query():
    """Execute a custom read-only analytics query."""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({'error': 'Query parameter required'}), 400
            
        query = data.get('query', '').strip()
        
        # Validate query is SELECT only
        if not is_select_query(query):
            return jsonify({'error': 'Only SELECT queries are allowed'}), 403
            
        df = execute_query(query)
        
        if df is None:
            return jsonify({'error': 'Failed to execute query'}), 500
            
        return jsonify({
            'rows': len(df),
            'data': df.to_dict(orient='records')
        }), 200
    except Exception as e:
        logger.error(f"Error executing custom query: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors gracefully."""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors gracefully."""
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Initialize database connection
    if not init_db_connection():
        logger.warning("Starting application without database connection")
    
    # Start Flask app
    debug_mode = os.getenv('DEBUG', 'False').lower() == 'true'
    port = int(os.getenv('PORT', 5000))
    
    try:
        app.run(
            host='0.0.0.0',
            port=port,
            debug=debug_mode
        )
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        exit(1)
