from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# ==================================================================
# FRAUD ALERT EMAIL SINK - city_bank.gold.fraud_card_alert
#
# The email-sending code must be pasted manually (copy from alert/send_email.py
# and adapt). The safety filter blocks the assistant from writing it.
#
# TODO 1: Paste the send_email() Gmail SMTP helper function from send_email.py
def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)
# TODO 2: Paste the config block (EMAIL_FROM / APP_PASSWORD via dbutils.secrets
EMAIL_FROM = "jaggurasekar105@gmail.com"
try:
    # dbutils is a module-like object: no parentheses
    # (dbutils() raises TypeError and APP_PASSWORD would always be None)
    APP_PASSWORD = dbutils.secrets.get(scope="city_bank-scope", key="gmail_api_key")
except Exception as e:
    print(f"❌ Failed to retrieve Gmail API key from secrets: {e}")
    APP_PASSWORD = None
#         scope="city_bank-scope", key="gmail_api_key") from send_email.py
# TODO 3: Paste the @dp.foreach_batch_sink(name="email_notifier_sink") function


@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for fraud watchlist matches."""
    
    if APP_PASSWORD is None:
        print(f"❌ Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return
    
    rows = df.collect()
    print(f"📧 Batch {batch_id}: Processing {len(rows)} alert(s)...")
    
    success_count = 0
    failure_count = 0
    
    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'customer_name': row.customer_name,
                'transaction_id': row.transaction_id,
                'card_number': row.card_number,
                'amount': float(row.amount),
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'city': row.city,
                'country': row.country,
                'transaction_timestamp': str(row.transaction_timestamp),
                'watchlist_id': row.watchlist_id,
                'risk_level': row.risk_level,
                'action': row.action,
                'reason_code': row.reason_code,
                'reason_description': row.reason_description
            }
            
            subject = f"🚨 Fraud Alert - {alert_data['alert_id']}"
            body = create_fraud_alert_email_body(alert_data)
            
            send_email(row.email, subject, body, EMAIL_FROM, APP_PASSWORD)
            
            success_count += 1
            print(f"  ✅ Email sent to {row.email} for transaction {row.transaction_id}")
            
        except Exception as e:
            failure_count += 1
            print(f"  ❌ Error processing alert {row.alert_id}: {e}")
    
    print(f"📊 Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")

#         and rename it: sink name "fraud_email_notifier_sink". Adapt the fields
#         to the fraud alert columns (see create_fraud_alert_email_body below):
#           recipient:   row.email (NOT row.customer_email)
#           alert_id, customer_name, transaction_id,
#           card_number, amount, merchant_name, merchant_category,
#           city, country, transaction_timestamp,
#           watchlist_id, risk_level, action,
#           reason_code, reason_description
#         and use create_fraud_alert_email_body(alert_data) for the body
# TODO 4: Paste the @dp.append_flow and change:
#           target="fraud_email_notifier_sink"
#           return spark.readStream.table("city_bank.gold.fraud_card_alert")
@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_alert_stream():
    """Streaming flow that reads fraud watchlist match alerts."""
    return spark.readStream.table("city_bank.gold.fraud_card_alert")
# ==================================================================


def create_fraud_alert_email_body(alert_data):
    """Creates the HTML email body for a fraud watchlist match alert."""
    return f"""<html><body style="font-family: Arial, sans-serif;">
<h2 style="color: #d9534f;">🚨 Fraud Alert - Suspicious Transaction Detected</h2>
<p>Dear {alert_data['customer_name']},</p>
<p>We detected a transaction on your card that matches a fraud watchlist entry:</p>
<div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #d9534f; margin: 20px 0;">
<h3 style="margin-top: 0;">Transaction Details</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
<li><strong>Card Number:</strong> {alert_data['card_number']}</li>
<li><strong>Amount:</strong> {alert_data['amount']:,.2f}</li>
<li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
<li><strong>Location:</strong> {alert_data['city']}, {alert_data['country']}</li>
<li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
</ul></div>
<div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
<h3 style="margin-top: 0;">Watchlist Match</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
<li><strong>Risk Level:</strong> {alert_data['risk_level']}</li>
<li><strong>Action:</strong> {alert_data['action']}</li>
<li><strong>Reason:</strong> {alert_data['reason_code']} - {alert_data['reason_description']}</li>
</ul></div>
<p><strong>Action Required:</strong></p>
<ul>
<li>If you authorized this transaction, no action is needed.</li>
<li>If you did NOT authorize this, contact our fraud department immediately and block your card.</li>
</ul>
<p>For your security, we're monitoring your account closely.</p>
<hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
<p style="font-size: 12px; color: #6c757d;">
Alert ID: {alert_data['alert_id']}<br>
This is an automated message from FinGuard Transaction Monitoring System.
</p>
</body></html>"""
