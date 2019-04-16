import os
import pandas as pd
from tabulate import tabulate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.email_plugin import EmailHook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    email_config = EmailHook()

    # Create message
    message = MIMEMultipart("alternative")
    message["Subject"] = "Leads report"
    message["From"] = email_config.sender
    message["To"] = email_config.receiver

    html = """
    <html>

    <head>
      <title> Facebook Report </title>
      <meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>

    </head>

    <body>

        <p>Blue</p>

    </body>
    """

    part1 = MIMEText(html, "html")

    message.attach(part1)

    email_config.send_message(message)


if __name__ == '__main__':
    main()
