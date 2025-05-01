import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import traceback

# Email configuration
sender_email = "weikwan214@gmail.com"
receiver_email = "weikwan214@gmail.com"

    
    
    
    
    


import smtplib
from email.mime.text import MIMEText

def send_email(subject, body, to_email, from_email, password, use_ssl=False):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    try:
        if use_ssl:
            # Use SSL on port 465
            # server = smtplib.SMTP_SSL('74.125.203.108', 465, timeout=30)
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30)
        else:
            # Use TLS on port 587
            server = smtplib.SMTP('74.125.203.109', 587, timeout=30)
            server.starttls()

        server.login(from_email, password)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully!")
        
    except TimeoutError as e:
        print(traceback.format_exc())
        print(f"Connection timed out: {str(e)}")
        print("Check your internet connection or try using SSL (port 465).")
    except Exception as e:
        print(traceback.format_exc())
        print(f"Failed to send email: {str(e)}")

# Example usage
if __name__ == "__main__":
    try:
        sender_email = "weikwan214@gmail.com"
        sender_password = "kcfuiipahudapoem"  # Use App Password if 2FA is enabled
        recipient_email = "weikwan214@gmail.com"
        email_subject = "Test Email from Python"
        email_body = "Hello! This is a test email sent from Python."

        # Try with TLS (port 587) first
        print("Trying with TLS (port 587)...")
        # send_email(email_subject, email_body, recipient_email, sender_email, sender_password, use_ssl=False)

        # If it fails, try with SSL (port 465)
        print("\nTrying with SSL (port 465)...")
        send_email(email_subject, email_body, recipient_email, sender_email, sender_password, use_ssl=True)


    except Exception as e:
        print(traceback.format_exc())
        print(f"Error: {e}")