#!/usr/bin/env python3
"""
Create Superset admin user if it doesn't exist.
This script is called after Superset is initialized and ready.
"""
import os
import sys
import time

# Add Superset to path and initialize app properly
sys.path.insert(0, '/app')

# Wait for Superset app to be initialized
max_retries = 30
retry_count = 0

while retry_count < max_retries:
    try:
        # Import after path is set
        from superset.app import create_app
        from superset.extensions import db
        from flask_appbuilder.security.sqla.models import User
        
        # Create the app instance
        app = create_app()
        
        with app.app_context():
            # Get admin user details from environment
            admin_username = os.getenv('SUPERSET_ADMIN_USER', 'admin')
            admin_password = os.getenv('SUPERSET_ADMIN_PASSWORD', 'admin')
            admin_email = os.getenv('SUPERSET_ADMIN_EMAIL', 'admin@example.com')
            admin_firstname = os.getenv('SUPERSET_ADMIN_FIRSTNAME', 'Admin')
            admin_lastname = os.getenv('SUPERSET_ADMIN_LASTNAME', 'User')
            
            # Check if admin user already exists
            existing_user = db.session.query(User).filter_by(username=admin_username).first()
            
            # Get the security manager from the app
            security_manager = app.appbuilder.sm
            
            if not existing_user:
                # Create admin user
                security_manager.add_user(
                    username=admin_username,
                    first_name=admin_firstname,
                    last_name=admin_lastname,
                    email=admin_email,
                    role=security_manager.find_role('Admin'),
                    password=admin_password
                )
                db.session.commit()
                print(f"✓ Created admin user: {admin_username}")
            else:
                # Update password if it was changed (only if password is provided and not default)
                if admin_password and admin_password != 'admin':
                    existing_user.password = security_manager.encrypt_password(admin_password)
                    db.session.commit()
                    print(f"✓ Updated password for admin user: {admin_username}")
                else:
                    print(f"Admin user {admin_username} already exists, skipping")
            break
    except ImportError as e:
        retry_count += 1
        if retry_count >= max_retries:
            print(f"Failed to import Superset modules after {max_retries} retries: {e}")
            exit(1)
        time.sleep(2)
    except Exception as e:
        retry_count += 1
        if retry_count >= max_retries:
            print(f"Failed to create admin user after {max_retries} retries: {e}")
            exit(1)
        time.sleep(2)

