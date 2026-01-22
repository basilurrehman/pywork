from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from email.message import EmailMessage

SCOPES = ["https://mail.google.com/"]
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
service = build("gmail", "v1", credentials=creds)

msg = EmailMessage()
msg["To"] = "yourprogway@gmail.com"
msg["From"] = "yourprogway@gmail.com"
msg["Subject"] = "Test Email"
msg.set_content("Hello! This is sent via Gmail API.")

encoded_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8").replace("\n","")
draft = service.users().drafts().create(userId="me", body={"message": {"raw": encoded_msg}}).execute()
print("📧 Draft created:", draft["id"])
