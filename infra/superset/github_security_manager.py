""" Custom Security Manager for GitHub OAuth authentication.
Does not store real names or emails - only GitHub username for identification. """

import os
import logging
from superset.security import SupersetSecurityManager


class CustomSecurityManager(SupersetSecurityManager):
    """Handles GitHub OAuth with org membership check and data anonymization."""

    def oauth_user_info(self, provider, response=None):
        """Retrieve user info from GitHub. Returns None if not in required org."""
        try:
            logging.info("oauth_user_info called with provider=%s", provider)

            if provider != "github":
                logging.info(
                    "Non-GitHub provider %s, using default oauth_user_info", provider
                )
                return super().oauth_user_info(provider, response)

            remote_app = self.appbuilder.sm.oauth_remotes[provider]

            me = remote_app.get("user")
            if me.status != 200:
                logging.error("GitHub /user request failed with status %s", me.status)
                return None

            data = me.json()
            username = data.get("login")
            logging.info("GitHub OAuth: authenticated user=%s", username)

            github_allowed_org = (os.getenv("SUPERSET_GITHUB_ALLOWED_ORG", "") or "").strip()
            if not github_allowed_org:
                logging.error(
                    "SUPERSET_GITHUB_ALLOWED_ORG is not configured. Authentication disabled."
                )
                raise ValueError(
                    "SUPERSET_GITHUB_ALLOWED_ORG environment variable is required but not set."
                )

            orgs_response = remote_app.get("user/orgs")
            if orgs_response.status != 200:
                logging.error(
                    "Failed to fetch organizations for user %s: HTTP %s",
                    username,
                    orgs_response.status,
                )
                return None

            user_orgs = [org.get("login", "").lower() for org in orgs_response.json()]
            logging.info("User %s orgs: %s", username, user_orgs)

            if github_allowed_org.lower() not in user_orgs:
                logging.info(
                    "User %s is NOT a member of required org %s",
                    username,
                    github_allowed_org,
                )
                return None

            logging.info(
                "User %s IS a member of required org %s",
                username,
                github_allowed_org,
            )

            return {
                "username": username,
                "email": f"github-{username}@anonymous.local",
                "first_name": username,
                "last_name": "",
            }

        except Exception:
            # This will log full stacktrace to the superset logs
            logging.exception("Error in CustomSecurityManager.oauth_user_info")
            return None
