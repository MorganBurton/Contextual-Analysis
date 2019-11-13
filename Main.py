#import modules
import os, requests, csv, webbrowser, re
import pandas as pd
import numpy as np
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup

#######
#create URLS for each yr/qtrs crawler file location
#######
def CreateCrawlerURLS():

	#initalise timeframe and url list
	beg_yr = 2001
	end_yr = 2001
	idx_urls = []
	#loop through timeframe
	for year in range(beg_yr, end_yr+1):

		for qtr in ['QTR1', 'QTR2', 'QTR3', 'QTR4']:

			#if not os.path.exists('./crawler-{}-{}/'.format(year, qtr)):

				#os.mkdir('./crawler-{}-{}/'.format(year, qtr))
			idx_url = 'https://www.sec.gov/Archives/edgar/full-index/{}/{}/crawler.idx'.format(year,qtr)
			idx_urls.append(idx_url)
	return idx_urls
	


#######
#retrieve and process crawlers
#######
def ProcessCrawler(url, df, header_loc=7,firstrow_loc=9):
	
	#get crawler.idx
	r = requests.get(url)
	lines = r.text.splitlines()
	
	#find columns
	name_loc = lines[header_loc].find('Company Name')
	type_loc = lines[header_loc].find('Form Type')
	cik_loc = lines[header_loc].find('CIK')
	date_loc = lines[header_loc].find('Date Filed')
	url_loc = lines[header_loc].find('URL')

	#create file names
	file_yr = url.split('/')[-3]
	file_qtr = url.split('/')[-2][-1]
	file_name = file_yr + "Q" + file_qtr + ".csv"

	with open(file_name, 'w') as wf:
		writer = csv.writer(wf, delimiter = ',')

		#remove junk
		for line in lines[firstrow_loc:]:

			company_name = line[:type_loc].strip()
			form_type = line[type_loc:cik_loc].strip()
			cik = line[cik_loc:date_loc].strip()
			date_filed = line[date_loc:url_loc].strip()
			page_url = line[url_loc:].strip()

			#identify/extract 10-k forms
			if form_type == '10-K':

				# create a new row of data using tuple which is ordered and unchanged
				row = [company_name, form_type, cik, date_filed, page_url]
				writer.writerow(row)

		print("{} saved".format(file_name))

		#save current crawler as a dataframe
		df = pd.read_csv(file_name, names=['company_name', 'form_type', 'cik', 'date_filed', 'page_url'])

	return df


	
	 #df = pd.DataFrame(csvreader, columns = ['company_name', 'form_type', 'cik', 'date_filed', 'page_url'])

				#urlretrieve(idx_urls[counter], './crawler-{}-{}/test-{}-{}.csv'.format(year, qtr, year, qtr))
				#counter+=1
				

				#with open('crawler-2000-QTR1/test-2000-QTR1.idx', 'r+') as crawl:
					#print (crawl.text)

#######
#retrieve and save 10-k for every company in every year/quarter
#######
def Parse10KPage(df):
	#scrape loop through each companys 10-k form page
	for x in range(0,len(df['page_url'])):

		res = requests.get(df['page_url'][x])
		print('10k page url', df['page_url'][x])
		#check url request response time
		print('RESPONSE TIME', res.elapsed.total_seconds())
		html = res.text
		soup = BeautifulSoup(html, 'html.parser')

		filer_div = soup.find('div', {'id': 'filerDiv'})
		filer_text = filer_div.find('span', {'class': 'companyName'}).find('a').get_text()
		filer_cik = re.search(r"(\d{10})\s(\(.+\))$" ,filer_text)[1]

		form_content = soup.find('div', {'class': 'formContent'})

		filing_date = form_content.find('div', text='Filing Date').findNext('div').get_text()
		report_date = form_content.find('div', text='Period of Report').findNext('div').get_text()

		table = soup.find('table', {'class': 'tableFile', 'summary': 'Document Format Files'})
		href = table.find('td', text='10-K').find_parent('tr').find('a')['href']
		print('10 k LINK', "https://www.sec.gov" + href)
		#if there is no 10-k filing return none
		if re.search('.txt', href):

			form_url = "https://www.sec.gov" + href
			print("SUCCESS", form_url)
		else:
			form_url = None
			print ('NO 10-K')
			
	

	return filer_cik, filing_date, report_date, form_url


df = []
idx_urls = CreateCrawlerURLS()
#loop through each year/qtr 
for url in idx_urls:
	
	df = ProcessCrawler(url, df)
	Parse10KPage(df)



#Parse_10K()








