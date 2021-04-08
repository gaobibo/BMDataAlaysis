import csv
import errno
import itertools
import os
import time

from bs4 import BeautifulSoup
import click
import dataset
import funcy as fy
import requests

from pyquery import PyQuery
import time
import traceback

import numpy as np
import pandas as pd

import seaborn as sns

from datetime import datetime
import time

state_id = 0
gender = 0
number_of_results = 40000
headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://registration.baa.org',
        'Referer': 'http://registration.baa.org/2016/cf/Public/iframe_ResultsSearch.cfm?mode=results',
    }
params = {
        'mode': 'results',
        'criteria': '',
        'StoredProcParamsOn': 'yes',
        'VarGenderID': gender,
        'VarBibNumber': '',
        'VarLastName': '',
        'VarFirstName': '',
        'VarStateID': state_id,
        'VarCountryOfResID': 0,
        'VarCountryOfCtzID': 0,
        'VarReportingSegID': 1,
        'VarAwardsDivID': 0,
        'VarQualClassID': 0,
        'VarCity': '',
        'VarTargetCount': number_of_results,
        'records': 25,
        'headerexists': 'Yes',
        'bordersize': 0,
        'bordercolor': '#ffffff',
        'rowcolorone': '#FFCC33',
        'rowcolortwo': '#FFCC33',
        'headercolor': '#ffffff',
        'headerfontface': 'Verdana,Arial,Helvetica,sans-serif',
        'headerfontcolor': '#004080',
        'headerfontsize': '12px',
        'fontface': 'Verdana,Arial,Helvetica,sans-serif',
        'fontcolor': '#000099',
        'fontsize': '10px',
        'linkfield': 'FormattedSortName',
        'linkurl': 'OpenDetailsWindow',
        'linkparams': 'RaceAppID',
        'queryname': 'SearchResults',
        'tablefields': 'FullBibNumber,FormattedSortName,AgeOnRaceDay,GenderCode,'
                       'City,StateAbbrev,CountryOfResAbbrev,CountryOfCtzAbbrev,'
                       'DisabilityGroup',
    }

results = []
start = 1
page_number = 1
#for page_number, start2 in enumerate(itertools.count(1, 25)):
while True:
        # Don't hammer the server. Give it a sec between requests.
        time.sleep(1.0)

        click.echo('page %d of %d' % (page_number + 1, number_of_results/25))
        try:
            response = requests.post(
                'http://registration.baa.org/2019/cf/Public/iframe_ResultsSearch.cfm',
                headers=headers,
                params=params,
                data={'start': start, 'next': 'Next 25 Records'},
            )
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find("table", attrs={"class": "tablegrid_table"})
            if table == None:
                print("table is none")
                continue
            rows = table.findAll("tr")
            for row in rows:
                a = [t.text.strip() for t in row.findAll("td")][0:]
                #Don't store lines without data
                if len(a) > 0 and a != [''] and a !=['',''] and a != ['', '', '']: 
                    #print(a)
                    results.append(a)
            print(len(rows))
            #if page_number == 3:
            #    break

            # Only yield if there actually are results. Just found this random
            # tr_header thing in the HTML of the pages that have results, but not
            # empty results pages.
            if 'tr_header' in response.text:
                (page_number, response.text)
            else:
                assert 'Next 25 Records' not in response.text
                click.echo('  No results found.')
                break

            # No more pages!
            if 'Next 25 Records' not in response.text:
                break
            
            start += 25
            page_number += 1
        except Exception as e:
                print (e)
                continue

print(len(results))
print(results[0])

data = []
for i, result in enumerate(results):
    if i%4 == 0:
        data.append(results[i] + results[i+1][1:])
        
print(len(data))
print(data[0])


columns = ['Bib', 'Name', 'Age', 'M/F', 'City', 'State', 'Country', 'Citizen', '', '5K', '10K', '15K', '20K', 'Half',
          '25K', '30K', '35K', '40K', 'Pace', 'Proj Time', 'Official Time', 'Overall', 'Gender', 'Division']

filename = 'bm_2019.csv'


df = pd.DataFrame(data)
df.to_csv(filename, index=False)