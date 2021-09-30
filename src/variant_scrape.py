import pandas as pd
from tableauscraper import TableauScraper as TS
from datetime import datetime, timedelta

url = "https://public.tableau.com/shared/RHJFBQBPJ?:toolbar=n&:display_count=y&:origin=viz_share_link&:embed=y"
ts = TS()
ts.loads(url)
workbook = ts.getWorkbook()

data = workbook.getCrossTabData('State Proportions')
data_out = data[data.State == 'Texas']

data_day = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
data_out['Date'] = data_day

write_path = 'C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources/historical/cdc_variants'
data_out.to_csv(f'{write_path}/cdc_variants_{data_day}.csv', index=False)
