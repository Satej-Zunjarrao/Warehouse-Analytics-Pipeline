"""
alerts_automation.py

This module handles real-time notifications for predefined thresholds.
It triggers alerts for warehouse managers when metrics exceed critical values.
Author: Satej
"""

import smtplib  # For sending email notifications
from email.mime.text import MIMEText  # For email message formatting
from email.mime.multipart import MIMEMultipart  # For multi-part email formatting
import pandas as pd  # For handling data

# SMTP configuration for email alerts
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_ADDRESS = "satej@example.com"  # Replace with your email
EMAIL_PASSWORD = "your_password"  # Replace with your password

def send_email_alert(subject, message, recipients):
    """
    Sends an email alert to the specified recipients.

    Args:
        subject (str): Subject line of the email.
        message (str): Body content of the email.
        recipients (list): List of email addresses to send the alert.
    """
    try:
        # Create email message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject

        # Attach the message content
        msg.attach(MIMEText(message, 'plain'))

        # Establish connection and send email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()

        print(f"Email alert sent successfully to: {recipients}")
    except Exception as e:
        print(f"Error sending email alert: {e}")

def check_thresholds_and_alerts(df):
    """
    Checks if any metrics exceed predefined thresholds and triggers alerts.

    Args:
        df (pd.DataFrame): DataFrame containing metrics to evaluate.
    """
    try:
        # Example thresholds
        LOW_INVENTORY_THRESHOLD = 100
        HIGH_UTILIZATION_THRESHOLD = 90  # In percentage

        # Filter rows with critical conditions
        low_inventory = df[df['Total Inventory'] < LOW_INVENTORY_THRESHOLD]
        high_utilization = df[df['inventory_utilization'] > HIGH_UTILIZATION_THRESHOLD]

        # Trigger alerts for low inventory
        for _, row in low_inventory.iterrows():
            message = f"Alert: Inventory in Warehouse {row['Warehouse ID']} is critically low at {row['Total Inventory']} units."
            send_email_alert(
                subject="Low Inventory Alert",
                message=message,
                recipients=["manager@satejwarehouse.com"]
            )

        # Trigger alerts for high storage utilization
        for _, row in high_utilization.iterrows():
            message = f"Alert: Warehouse {row['Warehouse ID']} has high storage utilization at {row['inventory_utilization']}%."
            send_email_alert(
                subject="High Utilization Alert",
                message=message,
                recipients=["manager@satejwarehouse.com"]
            )

        print("Threshold checks and alerts completed.")
    except Exception as e:
        print(f"Error during threshold checks and alerts: {e}")

if __name__ == "__main__":
    # Example usage
    dashboard_data_path = "/home/satej/data/dashboard_ready_data.csv"
    dashboard_data = pd.read_csv(dashboard_data_path)

    # Perform threshold checks and send alerts
    check_thresholds_and_alerts(dashboard_data)
