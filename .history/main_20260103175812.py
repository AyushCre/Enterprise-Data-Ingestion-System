"""
Secure Enterprise Batch Data Pipeline with Authentication
"""

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from pathlib import Path
from ui.dashboard import render_dashboard

# === AUTHENTICATION SETUP ===
try:
    # Load config
    auth_config_path = Path("auth_config.yaml")
    with open(auth_config_path) as file:
        config = yaml.safe_load(file)

    # Create authenticator
    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days']
    )

    # Try to get existing login
    name, authentication_status, username = authenticator.login('Login', 'main')

    if authentication_status == False:
        st.error(f'Username/password are incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password.')
    elif authentication_status:
        # === SUCCESSFUL LOGIN ===

        # Header with logout
        st.markdown("### 🔐 Enterprise Batch Data Pipeline")
        st.caption(f"Welcome {name}")
        authenticator.logout('Logout', 'main')

        # Render main dashboard
        render_dashboard()

except Exception as e:
    st.error(f"Auth config error: {e}")
    st.info("Running without authentication for development.")
    render_dashboard()
