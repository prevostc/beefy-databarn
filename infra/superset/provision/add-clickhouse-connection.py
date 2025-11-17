#!/usr/bin/env python3
"""
Add ClickHouse database connection to Superset.
This script is called after Superset is initialized and ready.
"""
import os
import sys
import time

# Add Superset to path before any imports
sys.path.insert(0, '/app')

# Get connection details from environment
clickhouse_host = os.getenv('CLICKHOUSE_HOST', '') or 'clickhouse'
clickhouse_port = os.getenv('CLICKHOUSE_PORT', '') or '8123'
clickhouse_user = os.getenv('CLICKHOUSE_USER', '') or 'default'
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '') or ''
clickhouse_db = os.getenv('CLICKHOUSE_DB', '') or 'analytics'
sqlalchemy_uri = f"clickhousedb://{clickhouse_user}:{clickhouse_password}@{clickhouse_host}:{clickhouse_port}/{clickhouse_db}"
print(sqlalchemy_uri)

# Wait for Superset app to be initialized
max_retries = 1
retry_count = 0

while retry_count < max_retries:
    # try:
    # Import after path is set and within retry loop to handle import errors
    from superset.app import create_app
    from superset.extensions import db
    from superset.models.core import Database
    from sqlalchemy.exc import IntegrityError
    
    # Create the app instance
    app = create_app()
    
    with app.app_context():
        # Check if database connection already exists
        existing_db = db.session.query(Database).filter_by(database_name='ClickHouse').first()
        
        if not existing_db:
            # Create new database connection
            database = Database(
                database_name='ClickHouse',
                sqlalchemy_uri=sqlalchemy_uri,
                extra='{"engine": "clickhouse"}'
            )
            db.session.add(database)
            try:
                db.session.commit()
                print(f"✓ Added ClickHouse database connection: {clickhouse_host}:{clickhouse_port}/{clickhouse_db}")
            except IntegrityError:
                db.session.rollback()
                print("ClickHouse database connection already exists")
        else:
            print("ClickHouse database connection already exists, skipping")
        break
    # except ImportError as e:
    #     retry_count += 1
    #     if retry_count >= max_retries:
    #         print(f"Failed to import Superset modules after {max_retries} retries: {e}")
    #         exit(1)
    #     time.sleep(2)
    # except Exception as e:
    #     retry_count += 1
    #     if retry_count >= max_retries:
    #         print(f"Failed to add ClickHouse connection after {max_retries} retries: {e}")
    #         exit(1)
    #     time.sleep(2)

