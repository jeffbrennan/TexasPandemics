import yaml

import os
import glob
import requests
import sys
import json
import time
import smtplib
import pandas as pd
from datetime import datetime as dt
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


# sources
# https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
# https://www.tutorialspoint.com/send-mail-with-attachment-from-your-gmail-account-using-python
def build_file_details(receiver, file, get_values):
    body = []

    base_text = (
        f'<br><b>{file["Name"]}</b>'
        f'<br>Regional Level: {receiver["region_level"]}'
        f'<br>Regions: {", ".join(receiver["regions"])}'
        f'<br>Date: {file["Date"]}'
    )

    if get_values == 'yes':
        text_part = (
            f'{base_text}'
            f'<br>Value (95% CI): {file["Value"]} ({file["Low_CI"]}, {file["Upper_CI"]})'
        )

    body.append(text_part)
    output = ''.join(body)
    return output


def build_body_text(receiver, file_details, credentials):
    message = (
        f'Hello,<br>Here is your requested data:<br>'
        f'{"<br>".join(file_details)}'
        f'<br>Notes: {receiver["notes"]}'
        f'<br><br>This is a bot. Please contact {credentials["real_email"]} if you have any questions.'
    )

    return message


def add_attachments(message, receiver) -> MIMEMultipart:
    if receiver['send_files'] == "no":
        return message

    file_paths = [file[1] for file in receiver['files']]
    for path in file_paths:
        part = MIMEBase('application', 'octate-stream')

        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
        message.attach(part)

    return message


def build_message(credentials, receiver, get_values, parsed_files) -> str:
    message = MIMEMultipart()
    message['From'] = credentials['sender']
    message['To'] = ','.join(receiver['address'])
    message['Subject'] = f'{receiver["nickname"]} {receiver["data_type"]} [{parsed_files[0]["Date"]}]'

    message['Cc'] = ','.join(receiver['cc'])

    # for each .csv, construct file details and combine together into message out
    file_details = [build_file_details(receiver, file, get_values) for file in parsed_files if file]
    message_out = build_body_text(receiver, file_details, credentials)

    message.attach(MIMEText(message_out, 'html'))
    message_with_attachments =  add_attachments(message, receiver)

    output = message_with_attachments.as_string()
    return output

def create_authenticated_smtp_session(credentials):
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()
    session.login(credentials['sender'], credentials['sender_pass'])
    return session

def prepare_email(credentials, receiver, parsed_files, get_values) -> dict:
    message_text = build_message(credentials, receiver, get_values, parsed_files)
    session = create_authenticated_smtp_session(credentials)

    # convert parsed message to string, add receivers
    output = {
        'session': session,
        'credentials': credentials,
        'all_receivers': receiver['address'] + receiver['cc'],
        'text': message_text
    }

    return output


def send_email(message_info: dict) -> None:
    session = message_info['session']
    session.sendmail(
        message_info['credentials']['sender'],
        message_info['all_receivers'],
        message_info['text']
    )
    session.quit()


# extract most recent values from files to be used in email body text
def parse_file(file, get_values):
    file_name = file[0]
    file_path = file[1]

    if not file_path.endswith('csv'):
        return None

    # assumes format Date, value low CI, upper CI
    df = pd.read_csv(file_path)
    max_date = max(df['Date'])

    if get_values == 'no':
        return {'Name': file_name, 'Date': max_date}

    df_new = df[(df.Date == max_date)]

    value = round(df_new.iloc[0, 1], 3)
    low_CI = round(df_new.iloc[0, 2], 3)
    upper_CI = round(df_new.iloc[0, 3], 3)

    parsed_data = {
        'Name': file_name,
        'Date': max_date,
        'Value': value,
        'Low_CI': low_CI,
        'Upper_CI': upper_CI
    }

    return parsed_data


def handle_receiver(receiver: dict, credentials: dict) -> str:
    try:
        get_values = receiver['get_values']
        parsed_files = [parse_file(file, get_values) for file in receiver['files']]
        message_info = prepare_email(credentials, receiver, parsed_files, get_values)
        send_email(message_info)
        output_message = f'Successfully sent email to {receiver["nickname"]}'
    except Exception as e:
        output_message = (f'Error sending email to {receiver["nickname"]}: {e}')
    return output_message


def determine_if_new(receiver: dict) -> dict | None:
    start_time = time.time()
    # max 20 minutes between file modification and send, else won't send
    max_modification_lag_time = 60 * 20

    file_time = os.path.getmtime(receiver['files'][0][1])
    time_delta = start_time - file_time

    if time_delta < max_modification_lag_time:
        return receiver
    else:
        return None


def load_config() -> tuple[dict, dict]:
    config = yaml.safe_load(open('backend/test_email_config.yaml'))
    credentials = config['sender_config']
    all_recipients = config['receiver_config']
    return credentials, all_recipients


def main() -> None:
    credentials, all_recipients = load_config()
    recipients = [determine_if_new(all_recipients[receiver]) for receiver in all_recipients]

    if not recipients:
        sys.exit()

    response = [handle_receiver(receiver, credentials) for receiver in recipients]
    print(response)

if __name__ == '__main__':
    main()
