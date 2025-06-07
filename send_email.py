# Import necessary libraries
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from getpass import getpass # To securely get password
from config import settings

def send_email(sender_email, app_password, receiver_email, subject, body):
    """
    Prompts the user for email details and sends an email.
    """
    # --- Configuration ---
    # You might need to change the smtp_server and port depending on your email provider
    # Common SMTP servers and ports:
    # Gmail: smtp.gmail.com, port 587 (TLS) or 465 (SSL)
    # Outlook/Hotmail: smtp-mail.outlook.com, port 587 (TLS)
    # Yahoo: smtp.mail.yahoo.com, port 587 (TLS) or 465 (SSL)
    # iCloud: smtp.mail.me.com, port 587 (TLS)
    smtp_server = "smtp.gmail.com"  # Defaulting to Gmail, change if needed
    port = 587  # For starttls

    # # --- Get Email Details from User ---
    # sender_email = input("Enter your email address: ")
    # # Use getpass to hide password input
    # password = getpass("Enter your email password: ")
    # receiver_email = input("Enter receiver's email address: ")
    # subject = input("Enter the subject of the email: ")
    # body = input("Enter the body of the email (plain text): ")

    # --- Create the Email Message ---
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    # Create the plain-text version of your message
    text_part = MIMEText(body, "plain")

    # Add plain-text part to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(text_part)

    # --- Send the Email ---
    try:
        # Create a secure SSL context
        context = ssl.create_default_context()

        # Connect to the SMTP server
        # Using a 'with' statement ensures the connection is automatically closed
        with smtplib.SMTP(smtp_server, port) as server:
            print(f"Connecting to {smtp_server}...")
            # Start TLS for security (Transport Layer Security)
            server.starttls(context=context)
            print("Connection secured with TLS.")

            # Login to the email account
            print(f"Logging in as {sender_email}...")
            server.login(sender_email, app_password)
            print("Login successful.")

            # Send the email
            print(f"Sending email to {receiver_email}...")
            server.sendmail(sender_email, receiver_email, message.as_string())
            print("Email sent successfully!")

    except smtplib.SMTPAuthenticationError:
        print("SMTP Authentication Error: Could not login. Please check your email and password.")
        print("If using Gmail, you might need to: ")
        print("1. Enable 'Less secure app access' (not recommended for long-term use).")
        print("2. Or, generate and use an 'App Password'.")
    except smtplib.SMTPServerDisconnected:
        print("SMTP Server Disconnected: The server unexpectedly disconnected.")
    except smtplib.SMTPConnectError:
        print(f"SMTP Connect Error: Failed to connect to the server at {smtp_server}:{port}.")
    except ssl.SSLError as e:
        print(f"SSL Error: {e}. This might be due to an issue with the SSL certificate or configuration.")
    except ConnectionRefusedError:
        print(f"Connection Refused: Ensure the SMTP server ({smtp_server}) and port ({port}) are correct and accessible.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    send_email(settings.GMAIL_USERNAME, settings.GMAIL_APP_PASSWORD, "david2001.du@gmail.com", "Test", "This is a test email.")
