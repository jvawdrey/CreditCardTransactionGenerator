from datetime import datetime
from random import Random
import copy
import pandas
import math
import csv
import json
import logging

NUM_RECORDS = 2000

# ---------- MERCHANT NAME ----------
def merchant_name(rnd, merchant_names_df):
    rec = merchant_names_df[['Name','trxn_mean','trx_std']].sample(1)
    name = rec['Name'].to_string(index=False)
    trxn_mean = rec['trxn_mean'].to_string(index=False)
    trx_std = rec['trx_std'].to_string(index=False)
    return (name, trxn_mean, trx_std)

# ---------- MERCHANT CITY, STATE ----------
def merchant_city_state(rnd, cities_states_df, restrictToStates):
    cols = ['City','State short','City alias','Longitude','Latitude']
    if (restrictToStates != []):
        rec = cities_states_df[cities_states_df['State short'].isin(restrictToStates)][cols].sample(1)
    else:
        rec = cities_states_df[cols].sample(1)
    city = rec['City'].to_string(index=False)
    alias = rec['City alias'].to_string(index=False)
    state = rec['State short'].to_string(index=False)
    long = rec['Longitude'].to_string(index=False)
    lat = rec['Latitude'].to_string(index=False)
    return (city, state, long, lat, alias)



def create_merchant_info(rnd, cities_states_df, merchant_names_df, restrictToStates):

    merchantInfo = {}

    merchantInfo['rlb_location_key'] = 1
    name, trxn_mean, trx_std = merchant_name(rnd, merchant_names_df)
    merchantInfo['merchant_name'] = name
    merchantInfo['merchant_trxn_mean'] = trxn_mean
    merchantInfo['merchant_trxn_std'] = trx_std
    city, state, long, lat, alias = merchant_city_state(rnd, cities_states_df, restrictToStates)
    merchantInfo['merchant_city'] = city
    merchantInfo['merchant_state'] = state
    merchantInfo['merchant_long'] = long
    merchantInfo['merchant_lat'] = lat
    merchantInfo['merchant_city_alias'] = alias
    merchantInfo['transaction_id'] = 0

    return merchantInfo


def merchantGenerator(numberOfRecords, dataFiles, restrictToStates=[]):

    generator = Random()
    generator.seed()

    merchant_names_df = pandas.read_csv(dataFiles['retailers'], sep='|')
    cities_states_df = pandas.read_csv(dataFiles['locations'], sep='|')

    results = []

    for i in range(0,numberOfRecords):

        merchant = create_merchant_info(generator, cities_states_df, merchant_names_df, restrictToStates)

        # check if merchant + city + merchant_city_alias + state already exists
        # TO DO: Change this - very slow
        drop = False
        merchantCnt = 0
        for res in results:
            if res['merchant_name'] == merchant['merchant_name']:
                if res['merchant_city'] == merchant['merchant_city_alias'] and res['merchant_city_alias'] == merchant['merchant_city_alias'] and res['merchant_state'] == merchant['merchant_state']:
                    drop = True
                else:
                    merchantCnt += 1

        if drop == False:
            merchant['rlb_location_key'] = merchantCnt
            results.append(merchant)

    msg = 'Merchant Generator: {} total location records created'.format(len(results))
    logging.info(msg)

    with open('locations.json', 'w') as location_file:
        json.dump(results, location_file)

    location_file.close()

if __name__ == '__main__':
    dataFiles = {'retailers': 'us_retailers.csv', 'locations': 'us_cities_states_counties_longlats.csv'}
    merchantGenerator(NUM_RECORDS, dataFiles)
