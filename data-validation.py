import requests
import json
import pandas as pd
from datetime import datetime as dt
import re

date_out = dt.now().strftime("%Y-%m-%d %H:%M")
auth = pd.read_csv('backend/auth.csv', header=None)
scraping_output = open("scrape-sources.Rout", "r").read() 

slack_channel = auth.iloc[2,1]
slack_token = auth.iloc[3,1]


def post_slack_message(text, blocks = None):
    return requests.post('https://slack.com/api/chat.postMessage', 
    {
        'token': slack_token,
        'channel': slack_channel,
        'text': text,
        'blocks': json.dumps(blocks) if blocks else None
    }).json()	


def build_run_block(status, date):
    blocks = [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "Script run at *" + date + "* EST"
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": status[0][0] + " *covid-scraping.rmd*" + status[0][1]
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": status[1][0] + " *statistical-analysis.rmd*" + status[1][1]
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": status[2][0] + " *diagnostics.rmd*" + status[2][1]
			}
		}
	]
    return blocks


def get_error_message(f):
    location = re.findall(r"(?<=Quitting from lines )(\d+-\d+)", f)[0]
    message = re.findall(r"(?=Error)(.*)", f)[0]
    message_out = '\nLocation: Lines ' + location + '\n' + message
    return message_out
    

def parse_scraping_output(f):
    # TODO: refactor    
    if ('Output created: covid-scraping.html' in f):
        scrape = [':heavy_check_mark:', '']

        if ('Output created: statistical-analysis.html' in f): 
            stats = [':heavy_check_mark:', '']

            if ('Output created: diagnostics.html' in f):
                diagnostics = [':heavy_check_mark:', '']
            else:
                diagnostics = [':x:', get_error_message(f)]
        else: 
            stats = [':x:', get_error_message(f)]
            diagnostics = [':x:', '\nStatistical analysis file failed']
    else: 
        scrape = [':x:', get_error_message(f)]
        stats = [':x:', '\nScraping file failed']
        diagnostics = [':x:', '\nScraping file failed']

    return [scrape, stats, diagnostics]


# TODO
# Parse validation text

def main():
    scraping_status = parse_scraping_output(scraping_output)
    blocks = build_run_block(scraping_status, date_out)
    post_slack_message(date_out + ' update', blocks)

if __name__ == '__main__':
    main()