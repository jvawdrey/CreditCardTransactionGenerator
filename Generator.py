#!/usr/bin/python

import yaml
import os
import json
import logging
import sys

import accountGenerator as ag
import merchantGenerator as mg
import transactionGenerator as tg

YAML_FILE="myConfigs.yml"
LOGGING_LEVEL=logging.INFO

def readYaml(filename):
    with open(filename, 'r') as stream:
        try:
            myConfigs = yaml.load(stream)
            return True, myConfigs
        except yaml.YAMLError as exc:
            return False, exc

def checkAndDropFile(filename):
    if os.path.exists(filename):
        os.remove(filename)
        return True
    else:
        return False

if __name__ == '__main__':

    # logging configurations
    logging.basicConfig(format='%(asctime)s - %(message)s',level=LOGGING_LEVEL)

    # read configs
    status, myConfigs = readYaml(YAML_FILE)
    if (status == False):
        logging.critical(myConfigs)
        sys.exit()

    try:
        restrictToStates = myConfigs['constraints']['states']
    except Exception as e:
        logging.error(e)
        restrictToStates = []

    if myConfigs['configs']['createAccountsJson'] == True:
        # clean up files if already exist
        checkAndDropFile('accounts.json')
        # create accounts.json file
        ag.accountGenerator(myConfigs, restrictToStates)

    if myConfigs['configs']['createLocationsJson'] == True:
        # clean up files if already exist
        checkAndDropFile('locations.json')
        # create locations.json file
        mg.merchantGenerator(myConfigs['generator']['merchantsNumber'], myConfigs['data'], restrictToStates)

    # simulate transactions
    if (myConfigs['target']['type'] == 'kafka'):
        tg.generate_kafka_data(myConfigs)

    elif (myConfigs['target']['type'] == 'json'):
        tg.generate_file_data(myConfigs)
