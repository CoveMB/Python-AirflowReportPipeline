import ssl
import smtplib
from airflow import configuration
from airflow.hooks.base_hook import BaseHook


class EmailHook(BaseHook):

    # Hook to connect with FacebookAdsHook
    def __init__(self, email_conn_id='email_default'):

        # Link to the connection setting
        conn = self.get_connection(email_conn_id)

        # Getting Acces ID from connection
        self.password = conn.extra_dejson['password']
        self.sender = conn.extra_dejson['sender']
        self.receiver = conn.extra_dejson['receiver']

    def send_message(self, message):

        # For SS
        port = 465
        email_context = ssl._create_unverified_context()

        with smtplib.SMTP_SSL(
                "smtp.gmail.com", port,
                context=email_context) as server:
            server.login(self.sender, self.password)
            server.sendmail(
                self.sender, self.receiver, message.as_string()
            )
