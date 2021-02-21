#!/usr/bin/env python3

# pip install --user pandas openpyxl

import pandas as pd
import numpy as np

from os import path
from shutil import copyfile

account_file = "Account"
pf_file = "Portfolio.xlsx"
split_info_file = "splits.csv"
generated_split_info_file = "gen-" + split_info_file
generated_poss_split_info_file = "gen-poss-" + split_info_file

pd.options.display.float_format = '{:,.2f}'.format

t = pd.read_excel(account_file, header=2, parse_dates=True)
t['Date'] = pd.to_datetime(t['Date'], format='%Y-%m-%d')
txins = t['Transaction Particular'].str.extract(r'(?P<TX_Type>Sale|Purchase) of (?P<Instrument>[A-Z.]*)')
txa = pd.concat([t, txins], axis=1)
total_interest_paid = txa[txa['Transaction Type'] == 'IN']['Amount'].sum()

tx = txa[~txa['Instrument'].isnull()]

ins = pd.DataFrame(tx['Instrument'].unique(), columns=['Instrument'])
ins['Base Instrument'] = ins['Instrument'].str[:-2]
irregular_instrument_bases = set(ins[ins['Instrument'].str[-1] != 'N']['Base Instrument'].to_numpy())  # N == regular
regular_instruments = set(ins[ins['Base Instrument'].isin(irregular_instrument_bases)]['Instrument'].to_numpy())

mapped_symbol = {}
mapped_instrument = {}


def getMappedInstrument(s):
    if s in mapped_instrument:
        return mapped_instrument[s]
    else:
        i = s if s in regular_instruments else s[:-2]
        mapped_symbol[i] = s
        mapped_instrument[s] = i
        return i


def setInstrument(df):
    df['Instrument'] = df['Symbol'].map(getMappedInstrument)


pd.options.mode.chained_assignment = None  # default='warn' # TODO: Fix this
tx['Symbol'] = tx['Instrument']
setInstrument(tx)

per_symbol = tx.groupby('Symbol')
splits_list = []


def addSplit(poss_split_date, symbol, poss_split_ratio):
    split = pd.DataFrame(data={'Date': poss_split_date, "Symbol": symbol, "Ratio": poss_split_ratio}, index=[0])
    splits_list.append(split)


def writeSplitInfoToFile(si_file):
    if splits_list:
        pd.concat(splits_list, ignore_index=True).to_csv(si_file, index=False)


for symbol, transactions in per_symbol:
    tx_iter = transactions.iterrows()
    last_price = next(tx_iter)[1]['Price']
    for _, row in tx_iter:
        if last_price / row['Price'] > 1.75:
            poss_split_ratio = round(last_price / row['Price'])
            print("Possible split in {} of 1:{} before {}".format(symbol, poss_split_ratio, row['Date']))
            poss_split_date = row['Date'] - pd.Timedelta(days=1)
            split_info = []
            addSplit(poss_split_date, symbol, poss_split_ratio)
        last_price = row['Price']
print()

writeSplitInfoToFile(generated_split_info_file)

if not path.exists(split_info_file):
    copyfile(generated_split_info_file, split_info_file)
    print("Split info file {} not found. Copying {}... Please inspect the split info file for any errors.".format(
        split_info_file, generated_split_info_file))

splits = pd.read_csv(split_info_file)
# splits['Date'] = pd.to_datetime(splits['Date'], format='%Y-%m-%d')

for _, split in splits.iterrows():
    s = tx[(tx['Symbol'] == split['Symbol']) & (tx['Date'] <= split['Date'])]
    s['No. of Shares'] = s['No. of Shares'] * split['Ratio']
    s['Price'] = s['Price'] / split['Ratio']
    tx.update(s)

tx

qty = tx['No. of Shares'] * tx['Amount'].transform(np.sign)
tx = tx.copy()
tx['Qty'] = qty

tx_filtered = tx
# tx_filtered = tx_filtered[tx_filtered['Date'] > np.datetime64('today') - pd.Timedelta(weeks=1)]
# tx_filtered = tx_filtered[tx_filtered['Date'] < np.datetime64('today') - pd.Timedelta(weeks=1)]
# tx_filtered = tx_filtered[tx_filtered['Date'] < np.datetime64('2021-02-01')]

qty_amount = tx_filtered[['Instrument', 'Qty', 'Amount']].groupby('Instrument', as_index=False).sum()
qty_amount['PPS'] = qty_amount['Amount'] / qty_amount['Qty']

last_traded_price = tx[['Instrument', 'Price']].groupby('Instrument').last().to_dict()['Price']

pf = None
pf_price = {}
sales_commission = 1.01133
qty_mismatched_symbols = []

if path.exists(pf_file):
    mismatched_symbols_found = False
    pf = pd.read_excel(pf_file, header=2, parse_dates=True)
    pf = pf.dropna()
    pf['Symbol'] = pf['Security'].str.extract(r'(?P<Instrument>[A-Z.]*)')
    pf['Sell Price'] = pf['Sales Proceeds'] / pf['Quantity']
    setInstrument(pf)
    pf_price = pf[['Instrument', 'Traded Price']].groupby('Instrument').last().to_dict()['Traded Price']
    sales_commission = (pf['Traded Price'] / pf['Sell Price'])[0]
    qty_amount_ins = qty_amount
    qty_amount_ins['Symbol'] = qty_amount_ins['Instrument'].map(lambda i: mapped_symbol[i])
    qty_amount_pf = pd.merge(qty_amount_ins, pf, on=['Symbol'], validate="1:1")
    pf_quantities = qty_amount_pf[['Symbol', 'Qty', 'Quantity']]
    mismatched_pf_quantities = pf_quantities[pf_quantities['Qty'] != pf_quantities['Quantity']]
    for _, row in mismatched_pf_quantities.iterrows():
        mismatched_symbols_found = True
        print("Quantity mismatch in {}. Possible misconfigured split - Quantities are {} vs {}".format(row['Symbol'],
                                                                                                       row['Qty'],
                                                                                                       row['Quantity']))
        poss_split_ratio_approx = row['Quantity'] / row['Qty']
        if (poss_split_ratio_approx == round(poss_split_ratio_approx)):
            last_date = tx[tx['Symbol'] == row['Symbol']].groupby('Symbol', as_index=False).last()['Date'][0]
            print("Possible recent split of 1:{} in {} after {}".format(round(poss_split_ratio_approx), row['Symbol'],
                                                                        last_date))
            addSplit(last_date, row['Symbol'], round(poss_split_ratio_approx))
        qty_mismatched_symbols.append(row['Symbol'])
    writeSplitInfoToFile(generated_poss_split_info_file)
    txp = tx
    txp['Approx. share value'] = txp['Amount'] / txp['Qty']
    for mmi in qty_mismatched_symbols:
        print("\nMismatched Symbol : {}".format(mmi))
        print(pf[pf['Symbol'] == mmi].to_string())
        print(qty_amount[qty_amount['Symbol'] == mmi].to_string())
        print(txp[txp['Symbol'] == mmi].to_string())
        print("\n")
    if not mismatched_symbols_found:
        print("No quantity mismatches found. Split info is likely up to date.\n")
else:
    print("\nPortfolio file {} does not exist. Save the Portfolio file as well for more accurate results\n".format(
        pf_file))

last_price = {**last_traded_price, **pf_price}

tx_filtered['Eff. Price'] = tx_filtered['Amount'] / tx_filtered['Qty']
tx_filtered['Last Price'] = tx_filtered['Instrument'].map(lambda i: last_price[i])
tx_filtered['Last Price - Effective'] = tx_filtered['Last Price'] * tx_filtered['Amount'].map(
    lambda a: sales_commission if a >= 0 else 2 - sales_commission)
tx_filtered_bs = tx_filtered.groupby(['Instrument', 'TX_Type']).agg(
    {'Qty': 'sum', 'Amount': 'sum', 'Last Price': 'last', 'Last Price - Effective': 'last'})
tx_filtered_bs.insert(2, 'Avg. Price', tx_filtered_bs['Amount'] / tx_filtered_bs['Qty'])
print(tx_filtered_bs.to_string())
print()

qty_amount['Last Price'] = qty_amount['Instrument'].map(lambda i: last_price[i])
qty_amount['Last Sell Price'] = qty_amount['Last Price'] / sales_commission
qty_amount['Current Value %'] = qty_amount['Last Sell Price'] / qty_amount['PPS'] * 100
qty_amount['Sales Proceeds'] = qty_amount['Last Sell Price'] * qty_amount['Qty']
qty_amount['Gain/Loss'] = qty_amount['Sales Proceeds'] - qty_amount['Amount']
print(qty_amount.sort_values('Gain/Loss', ascending=False).to_string())

total_purchased_amount = tx_filtered_bs[tx_filtered_bs['Amount'] >= 0]['Amount'].sum()
total_sold_amount = -tx_filtered_bs[tx_filtered_bs['Amount'] < 0]['Amount'].sum()

net_expense = qty_amount['Amount'].sum()
total_expense = net_expense + total_interest_paid
total_amount_transferred = -txa[txa['Transaction Type'] == "R"]['Amount'].sum()
pf_value = qty_amount['Sales Proceeds'].sum()
total_gain_loss = qty_amount['Gain/Loss'].sum()

print()

print("Total Purchase Amount: {0:,.2f}".format(total_purchased_amount))
print("Total Sold Amount: {0:,.2f}".format(total_sold_amount))
print(
    "Total Commission Approx. : {0:,.2f}".format((sales_commission - 1) * (total_purchased_amount + total_sold_amount)))

print("Net cost of portfolio: {0:,.2f}".format(net_expense))
print("Total interest paid: {0:,.2f}".format(total_interest_paid))
print("Total expense for the portfolio: {0:,.2f}".format(total_expense))
print("Total deposits: {0:,.2f}".format(total_amount_transferred))
print("Current Portfolio value: {0:,.2f}".format(pf_value))
print("Total Gain/Loss: {0:,.2f}".format(total_gain_loss))

# print(qty_amount.to_string())
