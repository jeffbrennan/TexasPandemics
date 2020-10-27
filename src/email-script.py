import requests
import os
import pandas as pd
import json
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime as dt

# sources
# https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
# https://www.tutorialspoint.com/send-mail-with-attachment-from-your-gmail-account-using-python


def post_slack_message(text, blocks = None):
    return requests.post('https://slack.com/api/chat.postMessage', 
    {
        'token': auth['slack_token'],
        'channel': auth['slack_channel'],
        'text': text,
        'blocks': json.dumps(blocks) if blocks else None
    }).json()	


def build_slack_message(email_details): 

    return [(f'{" & ".join(detail[0])} received data from {detail[1]} '
             f'file(s) regarding {detail[2]} in the following regions: \n{", ".join(detail[3])}')
            for detail in email_details]


def build_file_details(receiver, file):
    body = []    
    text_part = (f'<br><b>{file["Name"]}</b>'
                 f'<br>Regional Level: {receiver["region_level"]}'
                 f'<br>Regions: {", ".join(receiver["regions"])}'
                 f'<br>Date: {file["Date"]}'  
                 f'<br>Value (95% CI): {file["Value"]} ({file["Low_CI"]} {file["Upper_CI"]})')

    body.append(text_part)

    return ''.join(body)


def build_body_text(receiver, file_details): 

    message = (f'Hello,<br>Here is your requested data:<br>'
               f'{"<br>".join(file_details)}'
               f'<br>Notes: {receiver["notes"]}'
               f'<br><br>This is a bot. Please contact {credentials["real_email"]} if you have any questions.')

    return message


def send_email(credentials, receiver, file_paths, parsed_files):
    message = MIMEMultipart()
    message['From'] = credentials['sender']
    message['To'] = ','.join(receiver['address'])

    message['Subject'] = f'{receiver["nickname"]} {receiver["data_type"]} [{parsed_files[0]["Date"]}]'
                          
    message['Cc'] = ','.join(receiver['cc'])


    # for each .csv, construct file details and combine together into message out
    file_details = [build_file_details(receiver, file) for file in parsed_files if file]
    message_out = build_body_text(receiver, file_details)

    message.attach(MIMEText(message_out, 'html'))


    # attach files to email
    if receiver['send_files']:
        for path in file_paths :
            part = MIMEBase('application', 'octate-stream')

            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="{}"'.format(Path(path).name))
            message.attach(part)

    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(credentials['sender'], credentials['sender_pass']) #login with mail_id and password
   
    # convert parsed message to string, add receivers
    text = message.as_string()
    all_receivers = receiver['address'] + receiver['cc']

    # send email and end SMTP session
    session.sendmail(credentials['sender'], all_receivers, text)
    session.quit()


# extract most recent values from files to be used in email body text
def parse_file(file):
    file_name = file[0]
    file_path = file[1]

    # assumes format Date, value low CI, upper CI
    if file_path.endswith('csv'):
        df = pd.read_csv(file_path)
        max_date = max(df['Date'])
        df_new = df[(df.Date == max_date)]
        
        value = round(df_new.iloc[0, 1], 3)
        low_CI = round(df_new.iloc[0, 2], 3)
        upper_CI = round(df_new.iloc[0, 3], 3)

        file_values = {'Name':file_name, 'Date':max_date, 'Value':value,
                       'Low_CI':low_CI, 'Upper_CI': upper_CI}

    else: 
        file_values = None

    return file_values


# set wd
os.chdir(r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID')

# read in data
credentials = pd.read_csv('backend/email_credentials.csv').squeeze()
recipients = json.load(open('backend/mailing_list.json', 'r'))
auth = pd.read_csv('backend/auth.csv').squeeze() 

# send emails
email_details = []
for receiver in recipients:
    parsed_files = [parse_file(file) for file in receiver['files']]
    file_paths = [file[1] for file in receiver['files']]

    send_email(credentials, receiver, file_paths, parsed_files)
    email_details.append([receiver['address'], len(receiver['files']), receiver['data_type'], receiver['regions']])

# build & send slack message
text = build_slack_message(email_details)
post_slack_message(text)