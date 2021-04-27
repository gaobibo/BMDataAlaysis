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

resultFile = open("results2010_1.csv", "w+")

columns_result = ['result_id', 'place', 'bib', 'first_name','last_name', 'gender', 'city', 'state','country_code', 
                 'clock_time','chip_time', 'pace', 'age','age_percentage', 
                  #'division-520648-placement', 'division-520649-placement'
                 ]
            

results_sum = 200

def getResults(raceId, eventId):
    print("getResults:" + str(raceId) + "," + str(eventId))
    result_page = 1
    result_count = 0
    result_failure = 0
    while result_count < results_sum:
        time.sleep(1.0)
        #click.echo('page %d of %d' % (result_page, results_sum/5))
        try:
            params_result = {
                'event_id':eventId,
                'page': result_page,
                'results_per_page': 100,
                }
            response = requests.get(
                'https://runsignup.com/Rest/race/' + raceId + '/results/get-results',
                #headers=headers,
                params=params_result,
            )
            soup = BeautifulSoup(response.text, 'lxml')
            #print(response.text)
            results = soup.find("results")
            if results == None:
                break
                
            results = results.findAll("result")
            if len(results) == 0:
                result_failure += 1
                if result_failure > 10:
                    break
                else:
                    continue
            else:
                result_failure = 1

            for result in results:
                #print(result)
                fields = result.findAll('field')
                resultInfo = {}
                for field in fields:
                    name = field.find('name')
                    value = field.find('value')
                    if name != None and value != None:
                        data = value.text.strip()
                        data = data.replace(',',' ')
                        resultInfo[name.text.strip()] = data

                resultData = raceId + ',' + eventId + ','
                for column_result in columns_result:
                    if resultInfo[column_result] != None:
                        resultData += resultInfo[column_result] + ','
                    else:
                        resultData += ','
                #print(resultData)
                resultFile.write( resultData+ "\n")
                resultFile.flush()
                result_count += 1

            result_page += 1
        except Exception as e:
            print (e)
            result_page += 1
            continue



headers = {
    'Host': 'runsignup.com',
    'Accept': '*/*'
    }
raceFile = open("races2010_1.csv", "w+")

columns_race = ['race_id', 'name', 'fb_page_id', 'last_date', 'last_end_date', 'next_date', 'next_end_date', 'is_draft_race', 
           'is_private_race', 'is_registration_open', 'created', 'street', 'city',
          'state', 'zipcode', 'country_code']

eventFile = open("events2010_1.csv", "w+")
columns_event = ['event_id', 'name', 'distance', 'event_type']


page_number = 1
race_count = 1
number_of_results = 10000
failure_number = 0
failure_number2 = 0

while True:
    # Don't hammer the server. Give it a sec between requests.
    time.sleep(1.0)

    click.echo('page %d of %d' % (page_number, number_of_results/500))
    try:
        params = {
            'format':'xml',
            'page': page_number,
            'results_per_page': 500,
            'start_date':'2010-01-01',
            'events': 'T'
            }
        response = requests.get(
                        'https://runsignup.com/rest/races',
                        #headers=headers,
                        params=params,
                        #data={'start': start, 'next': 'Next 25 Records'},
                    )
        #print(response.text)
        soup = BeautifulSoup(response.text, 'lxml')
        races = soup.find("races")
        
        if races == None:
            print("table is none")
            failure_number += 1
            if failure_number > 10:
                break
            continue
        else:
            failure_number = 1
            
        rows = races.findAll("race")
        if len(rows) == 0:
            print(response.text)
            failure_number2 += 1
            if failure_number2 > 10:
                break
            else:
                continue
        else:
            failure_number2 = 1
        for row in rows:
            raceData = ""
            raceId = row.find("race_id").text.strip()
            for column in columns_race:
                t = row.find(columns_race)
                if t == None:
                    #print("")
                    raceData += ','
                else:
                    #print(t.text.strip())
                    data = t.text.strip()
                    data = data.replace(',',' ')
                    raceData += data + ','
            
            events = row.find('events')
            if events != None:
                events = events.findAll("event")
                for event in events:
                    eventInfo = raceId + ','
                    eventId = event.find("event_id").text.strip()
                    for column_event in columns_event:
                        t = event.find(column_event)
                        if t == None:
                            #print("")
                            eventInfo += ','
                        else:
                            #print(t.text.strip())
                            data = t.text.strip()
                            data = data.replace(',',' ')
                            eventInfo += data + ','
                    
                    getResults(raceId, eventId)
                    eventFile.write( eventInfo+ "\n")
                    eventFile.flush()
                    
                    
            raceFile.write( raceData+ "\n")
            raceFile.flush()
            race_count += 1
        page_number += 1
        if race_count >= number_of_results:
            break
    except Exception as e:
        print (e)
        page_number += 1
        continue

raceFile.close()
eventFile.close()
resultFile.close()
