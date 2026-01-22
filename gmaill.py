from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from email.message import EmailMessage

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
service = build("gmail", "v1", credentials=creds)

msg = EmailMessage()
msg["To"] = "basilurrehmann@gmail.com"
msg["From"] = "yourprogway@gmail.com"
msg["Subject"] = "Test Email"
msg.set_content("Hello! This is sent via Gmail API.")

encoded_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
service.users().messages().send(userId="me", body={"raw": encoded_msg,"labelIds": ["SENT"]}).execute()
print("📧 Email sent successfully!")
