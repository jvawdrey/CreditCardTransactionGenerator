#!/usr/bin/env python

import time
import json
import random
import numpy as np
import pandas as pd
import csv
import math
import sys
import logging
from datetime import timedelta,datetime
from kafka import SimpleProducer, KafkaClient
import geopy.distance
import enlighten

import fraudSignatures as fs

class myKafka:
    def __init__(self, kafkaClient, kafkaTopic):
        self.client = kafkaClient
        self.topic = kafkaTopic
        self.kafkaClient = None
        self.kafkaProducer = None

    def setClientAndTopic(client, topic):
        self.client = client
        self.topic = topic

    def connect(self):
        try:
            self.kafkaClient = KafkaClient('{}'.format(self.client))
            self.kafkaProducer = SimpleProducer(self.kafkaClient)
            # connection successful
            return (True, None)
        except Exception as e:
            # connection failed
            return (False, e)

    def send(self, message):
        try:
            self.kafkaProducer.send_messages(self.topic, json.dumps(message).encode('utf-8'))
            return (True, None)
        except Exception as e:
            return (False, e)

class myDataFiles:

    def __init__(self):

        # import locations data
        with open('locations.json') as data_file:
            locations = json.load(data_file)

        # prep locations data in with state for key
        uniqueStatesList = []
        for loc in locations:
            if(loc['merchant_state'] not in uniqueStatesList):
                 uniqueStatesList.append(loc['merchant_state'])
        uniqueStates = {}
        for state in uniqueStatesList:
            uniqueStates[state] = [loc for loc in locations if loc['merchant_state'] == state]

        # import accounts data
        with open('accounts.json') as data_file:
            accounts = json.load(data_file)

        self.locations = locations
        self.uniqueStatesList = uniqueStatesList
        self.uniqueStates = uniqueStates
        self.accounts = accounts

        # Setup progress bar
        manager = enlighten.get_manager()
        N = len(self.accounts)

        msg = "Mapping {} locations to {} accounts".format(len(self.locations), N)
        logging.info(msg)
        pbar = manager.counter(total=N, desc='Progress', unit='account')

        # build list of locations within "distance" of account holders home address
        self.accounts_location = {}
        for a in self.accounts:
            self.accounts_location[a['account_id']] = []
            for l in self.locations:
                dist = geopy.distance.vincenty((a['lat'], a['long']), (l['merchant_lat'],l['merchant_long'])).miles
                if (dist < a['transaction_radius']):
                    l['merchant_distance'] = dist
                    self.accounts_location[a['account_id']].append(l)
            pbar.update()

        manager.stop()

class myTimestamp():

    def __init__(self, minOpen=2, maxClose=23):
        self.timestamp = datetime.now()
        self.minOpen = minOpen
        self.maxClose = maxClose

    # To Do: Make this location specific
    def newTimestamp(self, loc):

        rnd = int(random.random() * 10)
        self.timestamp = self.timestamp + timedelta(seconds=rnd)

        dow = self.timestamp.weekday()
        days = ['mon','tue','wed','thu','fri','sat','sun']

        open = loc['{}{}'.format(days[dow],'_open')]
        close = loc['{}{}'.format(days[dow],'_close')]

        if self.timestamp.hour < self.minOpen:
            self.timestamp.replace(hour=self.minOpen)

        if self.timestamp.hour > self.maxClose:
            N = 24 - self.timestamp.hour + self.minOpen
            self.timestamp = self.timestamp.replace(minute=0, second=0) + timedelta(hours=N)

        if open == -1 or close == -1 or self.timestamp.hour < open or self.timestamp.hour > close:
            rnd = int(random.random() * 10)
            self.timestamp = self.timestamp + timedelta(minutes=rnd)
            return False

        return time.mktime(self.timestamp.timetuple())

def output_file(filename, records, type='json'):
    try:
        if type == 'csv':
            df = pd.read_json(json.dumps(records), orient='list')
            df.to_csv(filename,index=False,quoting=csv.QUOTE_NONNUMERIC)
        elif type == 'json':
            f = open(filename,'w')
            for rec in records:
                f.write(json.dumps(rec) + '\n')
            f.close()
    except Exception as e:
        logging.error(e)

def iterate_transaction_id(datafiles, transaction_id):
    # iterate transaction_id
    for i in range(0,len(datafiles.locations)):
        if datafiles.locations[i]['transaction_id'] == transaction_id:
            datafiles.locations[i]['transaction_id'] += 1

def random_location(datafiles, acct):

    # build list of locations within "distance" of account holders home address
    close_locations = []
    close_locations = datafiles.accounts_location[acct['account_id']]

    #close_locations = []
    #for l in datafiles.locations:
    #    dist = geopy.distance.vincenty((lat, long), (l['merchant_lat'],l['merchant_long'])).miles
    #    if (dist < distance):
    #        l['merchant_distance'] = dist
    #        close_locations.append(l)

    msg = "{} total location found within {} miles".format(len(close_locations),acct['transaction_radius'])
    logging.info(msg)

    if (close_locations != []):
        loc = random.choice(close_locations)

    # no locations found - looks within state
    elif (acct['state'] in datafiles.uniqueStatesList):
        msg = "No merchant found within {} miles - choosing location within state".format(acct['transaction_radius'])
        logging.info(msg)
        loc = random.choice(datafiles.uniqueStates[acct['state']])

    # final option - pick location at random
    else:
        msg = "No merchant found within {} miles or state {} - choosing random location".format(acct['transaction_radius'], acct['state'])
        logging.info(msg)
        loc = random.choice(datafiles.locations)

    return loc

def random_account(datafiles):
    return random.choice(datafiles.accounts)

def generate_transaction(datafiles, ts, fraud, storeFraudFlag):

    # transaction date

    # Grab random account
    acct = random_account(datafiles)

    # Grab random merchant location
    loc = random_location(datafiles, acct)
    iterate_transaction_id(datafiles, loc['transaction_id'])

    trxnTS = ts.newTimestamp(loc)
    if trxnTS == False:
        return False

    # Create transaction (account dependent amount) - 20%
    if (np.random.rand() < 0.2):
        trxn_amount = str(round(np.random.normal(acct['trxn_mean'], acct['trxn_std']), 2))

    # Create transaction (merchant dependent amount) - 80%
    else:
        trxn_amount = str(round(np.random.normal(float(loc['merchant_trxn_mean']), float(loc['merchant_trxn_std'])), 2))

    trxn = {
        'rlb_location_key': loc['rlb_location_key']
       ,'account_id': acct['account_id']
       ,'account_number': acct['account_number']
       ,'card_type': acct['card_type']
       ,'location_id': loc['location_id']
       ,'merchant_city': loc['merchant_city']
       ,'merchant_city_alias': loc['merchant_city_alias']
       ,'merchant_name': loc['merchant_name']
       ,'merchant_state': loc['merchant_state']
       ,'merchant_long': loc['merchant_long']
       ,'merchant_lat': loc['merchant_lat']
       ,'posting_date': time.time()
       ,'transaction_amount': trxn_amount
       ,'transaction_date': trxnTS
       ,'transaction_id': loc['transaction_id']
    }

    if (storeFraudFlag == True):
        trxn['fraud_flag'] = False

    # Update transaction if fraud case
    if (fraud == True):

        msg = "***** Generating fraud transaction *****"
        logging.info(msg)
        trxn = fs.transform(trxn, acct, loc)

    return trxn

def generate_kafka_data(myConfigs):

    transactionNumber = myConfigs['generator']['transactionNumber']
    everyNFraud = myConfigs['generator']['FraudEveryNTransactions']
    sleepBetweenIterations = myConfigs['generator']['sleepBetweenIterations']
    storeFraudFlag = myConfigs['generator']['storeFraudFlag']

    datafiles = myDataFiles()
    ts = myTimestamp()

    mk = myKafka(myConfigs['target']['kafka'], myConfigs['target']['topic'])
    status, message = mk.connect()
    if (status == False):
        logging.critical("Error connecting to kafka")
        logging.critical(message)
        sys.exit()

    logging.info("Transaction Generator: Applying fraud signature every {} transactions".format(everyNFraud))

    iter_counter = 0
    results = []

    for i in range(0,transactionNumber):
        iter_counter += 1

        # MOD
        fraud = False
        if (iter_counter % everyNFraud == 0):
            logging.info("***** Generating fraud record *****")
            fraud = True

        msg = generate_transaction(datafiles, ts, fraud, storeFraudFlag)
        if msg == False:
            iter_counter -= 1
        else:
            status, message = mk.send(msg)

            if (status == False):
                logging.error("Error sending message")
                logging.error(msg)
                logging.error(message)

            time.sleep(sleepBetweenIterations)

def generate_file_data(myConfigs):

    transactionNumber = myConfigs['generator']['transactionNumber']
    everyNFraud = myConfigs['generator']['FraudEveryNTransactions']
    sleepBetweenIterations = myConfigs['generator']['sleepBetweenIterations']
    transactionPerFile = myConfigs['target']['transactionPerFile']
    storeFraudFlag = myConfigs['generator']['storeFraudFlag']
    storeFraudFlag = myConfigs['generator']['storeFraudFlag']

    datafiles = myDataFiles()
    ts = myTimestamp()

    iter_counter = 0
    batch_counter = 0
    results = []

    logging.info("Transaction Generator: Applying fraud signature every {} transactions".format(everyNFraud))

    for i in range(0,transactionNumber):
        iter_counter += 1

        # MOD
        fraud = False
        if ((iter_counter % everyNFraud) == 0):
            fraud = True

        msg = generate_transaction(datafiles, ts, fraud, storeFraudFlag)
        if msg == False:
            iter_counter -= 1
        else:

            results.append(msg)

            if (iter_counter == transactionPerFile or i == transactionNumber-1):

                filename = 'transactions_{}.{}'.format((str(time.time())).replace('.', ''),myConfigs['target']['type'])
                locationFilename = '{}{}'.format(myConfigs['target']['transactionsFileLoctation'],filename)
                output_file(locationFilename, results, myConfigs['target']['type'])
                iter_counter = 0
                results = []
                batch_counter += 1

            time.sleep(sleepBetweenIterations)

if __name__ == '__main__':
    ts = myTimestamp()
