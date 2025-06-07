import smtplib
import ssl
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from config import settings

logger = logging.getLogger(__name__)

class EmailService:
    """
    A service class for handling email sending operations.
    """
    def __init__(self):
        """
        Initializes the EmailService with credentials from the global settings.
        """
        self.smtp_server = "smtp.gmail.com"
        self.port = 587  # For starttls
        self.sender_email = settings.GMAIL_USERNAME
        self.password = settings.GMAIL_APP_PASSWORD
        self.receiver_email = settings.RECEIVER_EMAIL
        
        if not all([self.sender_email, self.password, self.receiver_email]):
            raise ValueError("Email credentials (username, password, receiver) are not fully configured in settings.")

    def send_email(self, subject: str, body: str):
        """
        Sends an email with the given subject and body.

        Args:
            subject (str): The subject of the email.
            body (str): The body of the email (can be HTML or plain text).
        """
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.sender_email
        message["To"] = self.receiver_email

        # Attach the body to the email
        message.attach(MIMEText(body, "plain"))

        try:
            # Create a secure SSL context
            context = ssl.create_default_context()
            
            logger.info(f"Connecting to SMTP server {self.smtp_server}:{self.port}...")
            with smtplib.SMTP(self.smtp_server, self.port) as server:
                server.starttls(context=context)  # Secure the connection
                server.login(self.sender_email, self.password)
                server.sendmail(
                    self.sender_email, self.receiver_email, message.as_string()
                )
            logger.info(f"Email alert titled '{subject[:30]}...' sent successfully to {self.receiver_email}.")
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication Error: Failed to send email. Please check your Gmail username and app password. Error: {e}")
            raise
        except Exception as e:
            logger.error(f"An error occurred while sending email: {e}", exc_info=True)
            raise 