"""
Custom Security Manager for GitHub OAuth authentication.
Does not store real names or emails - only GitHub username for identification.
"""
import os
import logging
from superset.security import SupersetSecurityManager


class CustomSecurityManager(SupersetSecurityManager):
    """Handles GitHub OAuth with org membership check and data anonymization."""

    def oauth_user_info(self, provider, response=None):
        """Retrieve user info from GitHub. Returns None if not in required org."""
        if provider != "github":
            logging.info(f"User {username} is not using GitHub OAuth. Skipping org membership check.")
            return super().oauth_user_info(provider, response)

        logging.info(f"User {username} is using GitHub OAuth. Checking org membership.")

        remote_app = self.appbuilder.sm.oauth_remotes[provider]
        me = remote_app.get("user")
        
        if me.status != 200:
            return None
        
        data = me.json()
        username = data.get("login")
        
        github_allowed_org = (os.getenv("SUPERSET_GITHUB_ALLOWED_ORG", "") or "").strip()
        if not github_allowed_org:
            logging.error("SUPERSET_GITHUB_ALLOWED_ORG is not configured. Authentication disabled.")
            raise ValueError(
                "SUPERSET_GITHUB_ALLOWED_ORG environment variable is required but not set. "
                "Please configure it to restrict access to a specific GitHub organization."
            )
        
        try:
            orgs_response = remote_app.get("user/orgs")
            if orgs_response.status != 200:
                logging.error(f"Failed to fetch organizations for user {username}: HTTP {orgs_response.status}")
                return None
            
            user_orgs = [org.get("login", "").lower() for org in orgs_response.json()]
            if github_allowed_org.lower() not in user_orgs:
                logging.info(f"User {username} is not a member of required organization {github_allowed_org}: {str(user_orgs)}")
                return None
        except Exception as e:
            logging.error(f"Failed to check GitHub org membership for {username}: {e}")
            return None
        
        logging.info(f"User {username} is a member of required organization {github_allowed_org}: {str(user_orgs)}")
        return {
            "username": username,
            "email": f"github-{username}@anonymous.local",
            "first_name": username,
            "last_name": "",
        }


