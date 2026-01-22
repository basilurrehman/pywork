from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os

# Gmail send scope
SCOPES = ["https://mail.google.com/"]
# If token.json exists, use it
creds = None
if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)

# If no creds or expired, do OAuth
if not creds or not creds.valid:
    # Web application flow
    flow = Flow.from_client_secrets_file(
        "credential.json",
        scopes=SCOPES,
        redirect_uri="http://localhost"  # MUST match console
    )

    # Generate auth URL
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent"
    )

    print("\nOpen this URL in YOUR LOCAL BROWSER:\n")
    print(auth_url)

    # Copy the code from the redirect URL after allowing access
    code = input("\nPaste the AUTHORIZATION CODE here: ").strip()

    # Exchange code for tokens
    flow.fetch_token(code=code)
    creds = flow.credentials

    # Save token for future use
    with open("token.json", "w") as token_file:
        token_file.write(creds.to_json())

print("\n✅ OAuth successful, token.json saved.")
