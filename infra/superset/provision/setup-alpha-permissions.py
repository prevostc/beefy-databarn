#!/usr/bin/env python3
"""
Ensure Alpha role has SQL Lab permissions.
This script is called after Superset is initialized and ready.

-- select all current permissions for Alpha role
select r.name, p.name, vm.name
from ab_role r 
join ab_permission_view_role pvr on pvr.role_id = r.id
join ab_permission_view pv on pv.id = pvr.permission_view_id
join ab_permission p on p.id = pv.permission_id
join ab_view_menu vm on vm.id = pv.view_menu_id
where r.name = 'Alpha'
order by vm.name, p.name;

"""
import os
import sys
import time

# Add Superset to path before any imports
sys.path.insert(0, '/app')

# Wait for Superset app to be initialized
max_retries = 30
retry_count = 0

while retry_count < max_retries:
    try:
        # Import after path is set
        from superset.app import create_app
        from superset.extensions import db
        from flask_appbuilder.security.sqla.models import Role, Permission, PermissionView, ViewMenu
        
        # Create the app instance
        app = create_app()
        
        with app.app_context():
            # Refresh session to ensure we have latest data after superset init
            db.session.expire_all()
            
            # Find the Alpha role
            alpha_role = db.session.query(Role).filter_by(name='Alpha').first()
            
            if not alpha_role:
                print("Warning: Alpha role not found. Creating it...")
                alpha_role = Role(name='Alpha')
                db.session.add(alpha_role)
                db.session.commit()
                print("✓ Created Alpha role")
            else:
                # Refresh the role to get latest permissions
                db.session.refresh(alpha_role)
            
            # SQL Lab permissions that need to be granted
            # Format: (permission_name, view_menu_name)
            sql_lab_permissions = [
                ("can_read", "Query"),
                ("menu_access", "Query Search"),
                ("menu_access", "SQL Editor"),
                ("menu_access", "SQL Lab"),
                ("can_execute_sql_query", "SQLLab"),
                ("can_export_csv", "SQLLab"),
                ("can_get_results", "SQLLab"),
                ("can_read", "SQLLab"),
                ("menu_access", "Saved Queries"),
                ("can_export", "SavedQuery"),
                ("can_read", "SavedQuery"),
                ("can_write", "SavedQuery"),
                ("can_read", "SqlLabPermalinkRestApi"),
                ("can_write", "SqlLabPermalinkRestApi"),
                ("can_sqllab", "Superset"),
                ("can_sqllab_history", "Superset"),
                ("can_activate", "TabStateView"),
                ("can_delete", "TabStateView"),
                ("can_delete_query", "TabStateView"),
                ("can_get", "TabStateView"),
                ("can_migrate_query", "TabStateView"),
                ("can_post", "TabStateView"),
                ("can_put", "TabStateView"),
                ("all_query_access", "all_query_access"),
            ]
            
            # Find all SQL Lab permissions
            permissions_added = []
            for perm_name, view_menu_name in sql_lab_permissions:
                # Find permission and view menu
                permission = db.session.query(Permission).filter_by(name=perm_name).first()
                view_menu = db.session.query(ViewMenu).filter_by(name=view_menu_name).first()
                
                if permission and view_menu:
                    # Find the PermissionView that links them
                    perm_view = db.session.query(PermissionView).filter_by(
                        permission_id=permission.id,
                        view_menu_id=view_menu.id
                    ).first()
                    
                    if perm_view:
                        # Check if Alpha role already has this permission
                        if perm_view not in alpha_role.permissions:
                            alpha_role.permissions.append(perm_view)
                            permissions_added.append(perm_name)
                            print(f"✓ Added permission '{perm_name}' on '{view_menu_name}' to Alpha role")
                        else:
                            print(f"  Permission '{perm_name}' on '{view_menu_name}' already exists for Alpha role")
                    else:
                        print(f"Warning: PermissionView for '{perm_name}' on '{view_menu_name}' not found")
                else:
                    missing = []
                    if not permission:
                        missing.append(f"Permission '{perm_name}'")
                    if not view_menu:
                        missing.append(f"ViewMenu '{view_menu_name}'")
                    print(f"Warning: {' and '.join(missing)} not found in Superset")
            
            if permissions_added:
                db.session.commit()
                print(f"✓ Successfully provisioned {len(permissions_added)} SQL Lab permissions for Alpha role")
            else:
                print("✓ Alpha role already has all required SQL Lab permissions")
            
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
            print(f"Failed to setup Alpha permissions after {max_retries} retries: {e}")
            import traceback
            traceback.print_exc()
            exit(1)
        time.sleep(2)

