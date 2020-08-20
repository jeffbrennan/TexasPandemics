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
test_channel = auth.iloc[5,1]

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
def build_validation_block(status):
	status_emoji = {1:":heavy_check_mark:", 2:":warning:"}
	blocks = [
		############################### COUNTY #####################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*County Level Data (county.csv)*"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID19DailyCountyFatalityCountData.xlsx|" + status_emoji[status] + "Cases>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily cases: \n ○ Average % change: \n ○ % of counties reporting > 0 cases:"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID-19CumulativeTestsOverTimebyCounty.xlsx|" + status_emoji[status] + "Deaths>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily deaths: \n ○ Average % change: \n ○ % of counties reporting > 0 deaths:"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID-19CumulativeTestsOverTimebyCounty.xlsx|" + status_emoji[status] + "Tests>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily tests: \n ○ Average % change: \n ○ % of counties reporting > 0 tests:"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID-19CumulativeTestsOverTimebyCounty.xlsx|" + status_emoji[status] + "Google Mobility>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily cases: \n ○ Average % change: \n ○ % of counties reporting > 0 cases"
			}
		},
		{
			"type": "divider"
		},
		############################### TSA ########################################################
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*TSA Level Data (tsa.csv)*"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID-19HospitalizationsOverTimebyTSA.xlsx|" + status_emoji[status] + "Hospitalizations>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily hospitalizations: \n ○ Average % change:"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasHospitalCapacityoverTimebyTSA.xlsx|" + status_emoji[status] + "Hospital Capacity>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily capacity: \n ○ Average % change:"
			}
		},
		{
			"type": "divider"
		},
		{
		############################### PHR ########################################################

			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*PHR Level Data (phr.csv)*"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://apps.hhs.texas.gov/providers/directories/Texas_Nursing_Facilities_COVID_Summary.xls|" + status_emoji[status] + "Nursing Facilities>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily cases: \n ○ Average % change: \n ○ % of PHRs reporting > 0 cases"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://apps.hhs.texas.gov/providers/directories/Texas_Assisted_Living_Facilities_COVID_Summary.xls|" + status_emoji[status] + "Assisted Living Facilities>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n ○ Average daily cases: \n ○ Average % change: \n ○ % of PHRs reporting > 0 cases"
			}
		},
		{
			"type": "divider"
		},
		############################### STATE ######################################################

		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*State Level Data (phr.csv)*"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "<https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx|" + status_emoji[status] + "Demographics>"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " ○ Latest date: \n Age: \n Gender: \n Race:"
			}
		}
	]


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
	validation_df = pd.read_csv('statistical_output/diagnostics/validation-test.csv')
    scrape_block = build_scrape_block(scraping_status, date_out)
    post_slack_message(date_out + ' update', scrape_block)

	validation_block = build_validation_block(validation_df)
	post_slack_message('validation', validation_block)

if __name__ == '__main__':
    main()