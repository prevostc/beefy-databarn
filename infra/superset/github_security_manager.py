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

            # 1) Fetch /user
            me = remote_app.get("user")
            logging.info("GitHub /user HTTP %s", me.status_code)

            if me.status_code != 200:
                logging.error(
                    "GitHub /user request failed: status=%s body=%s",
                    me.status_code,
                    me.text,
                )
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

            # 2) Fetch /user/orgs
            orgs_response = remote_app.get("user/orgs")
            logging.info("GitHub /user/orgs HTTP %s", orgs_response.status_code)

            if orgs_response.status_code != 200:
                logging.error(
                    "Failed to fetch organizations for user %s: HTTP %s body=%s",
                    username,
                    orgs_response.status_code,
                    orgs_response.text,
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

            # Anonymized identity for Superset
            return {
                "username": username,
                "email": f"github-{username}@anonymous.local",
                "first_name": username,
                "last_name": "",
            }

        except Exception:
            logging.exception("Error in CustomSecurityManager.oauth_user_info")
            return None
