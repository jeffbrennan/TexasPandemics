import requests
import json
import pandas as pd
from datetime import datetime as dt
import re
import os

date_out = dt.now().strftime("%Y-%m-%d %H:%M")
auth = pd.read_csv('backend/auth.csv').squeeze()
scraping_output = open("scrape-sources.Rout", "r").read() 

def post_slack_message(text, blocks = None):
    return requests.post('https://slack.com/api/chat.postMessage', 
    {
        'token': auth['slack_token'],
        'channel': auth['slack_channel'],
        'text': text,
        'blocks': json.dumps(blocks) if blocks else None
    }).json()	


def build_scrape_block(status, date):
    blocks = [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "Script run at *" + date + " EST*"
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

# TODO: refactor
def build_validation_block(validation):
	# status_emoji = {1:":heavy_check_mark:", 2:":warning:"}
	blocks = [
####################################### COUNTY #####################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*County Level Data (county.csv)*"
			}
		},
####################################### CASES ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Cases']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Cases']['text']
			}
		},
####################################### DEATHS ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Deaths']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Deaths']['text']
			}
		},
####################################### TESTS ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Tests']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Tests']['text']
			}
		},
####################################### GOOGLE MOBILITY ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Mobility']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text":  validation['County']['Mobility']['text']
			}
		},
############################### TSA ################################################################
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*TSA Level Data (tsa.csv)*"
			}
		},
############################### HOSPITALIZATIONS ################################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_Total']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_Total']['text']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_ICU']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_ICU']['text']
			}
		},
############################### HOSPITAL CAPACITY ################################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_Total']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_Total']['text']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_ICU']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_ICU']['text']
			}
		},
# ############################### PHR ########################################################
# 		{
# 			"type": "section",
# 			"text": {
# 				"type": "mrkdwn",
# 				"text": "*PHR Level Data (phr.csv)*"
# 			}
# 		},
# ############################### NURSING ################################################################
# 		{
# 			"type": "section",
# 			"text": {
# 				"type": "mrkdwn",
# 				"text": status['phr']['nursing']['url']
# 			}
# 		},
# 		{
# 			"type": "section",
# 			"text": {
# 				"type": "mrkdwn",
# 				"text": status['phr']['nursing']['text']
# 			}
# 		},
# ############################### ALF ################################################################

# 		{
# 			"type": "section",
# 			"text": {
# 				"type": "mrkdwn",
# 				"text": status['phr']['alf']['url']
# 			}
# 		},
# 		{
# 			"type": "section",
# 			"text": {
# 				"type": "mrkdwn",
# 				"text": status['phr']['alf']['text']
# 			}
# 		},
# 		{
# 			"type": "divider"
# 		},
############################### STATE ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*State Level Data (stacked_demographics.csv)*"
			}
		},
############################### DEMOGRAPHICS ################################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Age']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Age']['text']
			}
		},
			{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Gender']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Gender']['text']
			}
		},
			{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Race']['url']
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Race']['text']
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


def main():
	# SCRAPING
	scraping_status = parse_scraping_output(scraping_output)
	scrape_block = build_scrape_block(scraping_status, date_out)
	post_slack_message(date_out + ' update', scrape_block)

	# VALIDATION
	try:
		validation = json.load(open('statistical-output/diagnostics/validation.json', 'r'))
		validation_block = build_validation_block(validation)
		post_slack_message('validation', validation_block)
		os.remove('statistical-output/diagnostics/validation.json')
	except FileNotFoundError:
		post_slack_message('Validation file not created - scraping failed')

		
if __name__ == '__main__':
    main()