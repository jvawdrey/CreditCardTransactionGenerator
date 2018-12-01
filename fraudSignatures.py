#!/usr/bin/env python

import logging
import numpy as np

def transform(transaction, account, merchant):

    if (account == None):
        logging.error("Fraud Signatures: No account or default account values found")
        return {}

    if (merchant == None):
        logging.error("Fraud Signatures: No merchant or default merchant values found")
        return {}

    if 'fraud_flag' in transaction:
        transaction['fraud_flag'] = True

    decRand = np.random.rand()
    if (decRand < 0.4):

        # large transaction amount for account
        trxn_amount = str(round(np.random.normal(account['trxn_mean'], account['trxn_std']) * account['trxn_mean'] * 1000, 2))
        transaction['transaction_amount'] = trxn_amount

    elif (decRand < 0.8):

        # large transaction amount for merchant
        trxn_amount = str(round(np.random.normal(merchant['merchant_trxn_mean'], merchant['merchant_trxn_std']) * merchant['merchant_trxn_mean'] * 1000, 2))
        transaction['transaction_amount'] = trxn_amount

    else:

        # bad merchant
        transaction['merchant_city'] = 'Moscow'
        transaction['merchant_state'] = 'Russia'
        transaction['merchant_name'] = 'ACME Hackers'

    return transaction
