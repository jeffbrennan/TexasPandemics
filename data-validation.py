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
test_channel = auth.iloc[4,1]

def post_slack_message(text, blocks = None):
    return requests.post('https://slack.com/api/chat.postMessage', 
    {
        'token': slack_token,
        'channel': test_channel,
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
				"text": validation['County']['Cases']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Cases']['text'][0]
			}
		},
####################################### DEATHS ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Deaths']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Deaths']['text'][0]
			}
		},
####################################### TESTS ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Tests']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Tests']['text'][0]
			}
		},
####################################### GOOGLE MOBILITY ######################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['County']['Mobility']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text":  validation['County']['Mobility']['text'][0]
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
				"text": validation['TSA']['Hosp_Total']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_Total']['text'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_ICU']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Hosp_ICU']['text'][0]
			}
		},
############################### HOSPITAL CAPACITY ################################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_Total']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_Total']['text'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_ICU']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['TSA']['Cap_ICU']['text'][0]
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
				"text": validation['State']['Age']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Age']['text'][0]
			}
		},
			{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Gender']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Gender']['text'][0]
			}
		},
			{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Race']['url'][0]
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": validation['State']['Race']['text'][0]
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
	scraping_status = parse_scraping_output(scraping_output)
	validation = json.load(open('statistical-output/diagnostics/validation.json', 'r'))
	
	scrape_block = build_scrape_block(scraping_status, date_out)
	post_slack_message(date_out + ' update', scrape_block)

	validation_block = build_validation_block(validation)
	post_slack_message('validation', validation_block)

if __name__ == '__main__':
    main()