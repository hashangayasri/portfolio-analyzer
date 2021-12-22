#!/usr/bin/env python3

# pip install --user pandas openpyxl

import pandas as pd
import numpy as np
import numpy_financial

from datetime import datetime
from os import path
from shutil import copyfile
import glob
import argparse

sales_commission = 1.01133

account_file = "Account"
pf_file = "Portfolio.xlsx"
otc_file = "IPO.csv"
split_info_file = "splits.csv"
skip_split_info_calculation=False
skip_quotes_file_generation=False
ignore_portfolio_last_price=False
print_qty_mismatched_symbol_info=False
ignore_filtered_out_transactions_for_price=False

parser = argparse.ArgumentParser(description='Provides an insight into stock portfolios')
parser.add_argument('tx_filter_end_date' , type=np.datetime64, nargs='?', default=None,
                    help='Ignore transactions made after this date')
parser.add_argument('tx_filter_start_date' , type=np.datetime64, nargs='?', default=None,
                    help='Ignore transactions made before this date')
parser.add_argument('--account_file', default=account_file, metavar=account_file,
                    help='Account file containing all the trasactions or the prefix of Account files containing all the transactions')
parser.add_argument('--pf_file', default=pf_file, metavar=pf_file,
                    help='Current Portfolio file containing last prices [Optional]')
parser.add_argument('--otc_file', default=otc_file, metavar=otc_file,
                    help='OTC/IPO file. This file contains purchases that arent recorded in the Account files.')
parser.add_argument('--split_info_file', default=split_info_file, metavar=split_info_file,
                    help='Split info file. Approximate split info file will be generated automatically.')
parser.add_argument('--skip_split_info_calculation', dest='skip_split_info_calculation', default=skip_split_info_calculation, action='store_true',
                    help='Skip deducing split info.')
parser.add_argument('--skip_quotes_file_generation', dest='skip_quotes_file_generation', default=skip_quotes_file_generation, action='store_true',
                    help='Skip generating the Yahoo Finance compatible quotes file.')
parser.add_argument('--ignore_portfolio_last_price', dest='ignore_portfolio_last_price', default=ignore_portfolio_last_price, action='store_true',
                    help='By default, last price from the portfolio is also used to update the last price. '
                    'When this is set, only the last transaction value will be used as the last price.')
parser.add_argument('--print_qty_mismatched_symbol_info', dest='print_qty_mismatched_symbol_info', default=print_qty_mismatched_symbol_info, action='store_true',
                    help='Print infomation about symbols with quantity mismatches.')
parser.add_argument('--ignore_filtered_out_transactions_for_price', dest='ignore_filtered_out_transactions_for_price', default=ignore_filtered_out_transactions_for_price, action='store_true',
                    help='By default, even the transactions outside the filter range are used to update the last price. '
                    'When this is set, only the transactions within the filtered range will be considered.')
parser.add_argument('--no_buy_after_date' , type=np.datetime64, nargs='?', default=None,
                    help='If set, ignore buy transactions after this date')
parser.add_argument('--day_reports', type=np.datetime64, required=False, action='append', # nargs='*',
                    help='Get day report for the given date(s)')
args = parser.parse_args()

print("Program Options:")
for variable, value in parser.parse_args()._get_kwargs():
    if value is not None:
        print("{} = {}".format(variable, value))

tx_filter_end_date=args.tx_filter_end_date
tx_filter_start_date=args.tx_filter_start_date
account_file=args.account_file
pf_file=args.pf_file
otc_file=args.otc_file
split_info_file=args.split_info_file
skip_split_info_calculation=args.skip_split_info_calculation
skip_quotes_file_generation=args.skip_quotes_file_generation
ignore_portfolio_last_price=args.ignore_portfolio_last_price
print_qty_mismatched_symbol_info=args.print_qty_mismatched_symbol_info
ignore_filtered_out_transactions_for_price=args.ignore_filtered_out_transactions_for_price
no_buy_after_date=args.no_buy_after_date
day_reports=args.day_reports

generated_split_info_file = "gen-" + split_info_file
generated_poss_split_info_file = "gen-poss-" + split_info_file

pd.options.display.float_format = '{:,.2f}'.format
date_format='%Y-%m-%d'
def getFormattedDate(dt64):
    return dt64.strftime(date_format)

def getOTCTrades(otc_file):
    if not path.exists(otc_file):
        return pd.DataFrame();
    otc_trades = pd.read_csv(otc_file)

    otctx = pd.DataFrame(otc_trades['Date'], columns=['Date'])
    otctx['Date'] = pd.to_datetime(otctx['Date'], format='%Y-%m-%d')
    otctx['Transaction Type'] = otc_trades['Quantity'].map(lambda q : 'B' if q >= 0 else 'S')
    otctx['Transaction Particular'] = otc_trades['Quantity'].map(lambda q : 'Purchase of ' if q >= 0 else 'Sale of ') + otc_trades['Symbol'] + '0000'
    otctx['No. of Shares'] = otc_trades['Quantity']
    otctx['Price'] = otc_trades['Price']
    otctx['Amount'] = otc_trades['Price'] * otc_trades['Quantity']
    otctx['TX_Type'] = otc_trades['Quantity'].map(lambda q : 'Purchase' if q >= 0 else 'Sell')
    otctx['Instrument'] = otc_trades['Symbol']
    if(len(otctx) > 0):
        print("Loaded {} OTC/IPO transactions from {}".format(len(otctx), otc_file))

    otc_buys = otc_trades[otc_trades['Quantity'] > 0]
    otc_deps = pd.DataFrame(otc_buys['Date'], columns=['Date'])
    otc_deps['Date'] = pd.to_datetime(otc_deps['Date'], format='%Y-%m-%d')
    otc_deps['Transaction Type'] = "R"
    otc_deps['Transaction Particular'] = "Transfer of " + (otc_buys['Price'] * otc_buys['Quantity']).astype(str) + " for the OTC/IPO TX of " + otc_buys['Symbol']
    otc_deps['No. of Shares'] = 0
    otc_deps['Price'] = 0
    otc_deps['Amount'] = - otc_buys['Price'] * otc_buys['Quantity']
    otc_deps['TX_Type'] = np.nan
    otc_deps['Instrument'] = np.nan

    all_otc_tx = pd.concat([otctx, otc_deps], ignore_index=True)
    # if(len(all_otc_tx) > 0):
    #     print(all_otc_tx)
    return all_otc_tx

def readAccountFile(account_file):
    t = pd.read_excel(account_file, header=2, parse_dates=True)
    t['Date'] = pd.to_datetime(t['Date'], format=date_format)
    txins = t['Transaction Particular'].str.extract(r'(?P<TX_Type>Sale|Purchase) of (?P<Instrument>[A-Z.]*)')
    return pd.concat([t, txins], axis=1)

def getAccountFileRanges(account_files):
    accountFileRanges=[]
    for af in account_files:
        if af.startswith("Account_Summary_of_"):
            continue
        txaf = readAccountFile(af)
        accountFileRanges.append(((txaf[~txaf['Instrument'].isnull()]['Date'].min(),txaf[~txaf['Instrument'].isnull()]['Date'].max()), txaf, af))
    accountFileRanges.sort(key=lambda tup: tup[0])
    return accountFileRanges

def combineTransactions(accountFileRanges, otherTransactions = None):
    last_end_date = None
    tx_parts = []
    print()
    for afr in accountFileRanges:
        ((start_date, end_date), txa, account_file_name) = afr
        print("Transaction file {} contains transactions of {} days from {} to {}".format(account_file_name, (end_date - start_date).days + 1, getFormattedDate(start_date), getFormattedDate(end_date)))
        if last_end_date is not None:
            print("Using transactions after the date of {} from the file {} - {} days considered".format(getFormattedDate(last_end_date), account_file_name, (end_date - last_end_date).days))
            txa = txa[txa['Date'] > last_end_date]
            if last_end_date < start_date:
                print("WARNING: Potential missing transactions between the dates {} and {}".format(getFormattedDate(last_end_date), getFormattedDate(start_date)))
            if last_end_date > end_date:
                print("WARNING: Transactions from the file {} are redundant as all of the transactions have been filtered out".format(account_file_name))
        else:
            print("Using all transactions from the file {}".format(account_file_name))
        last_end_date = end_date
        tx_parts.append(txa)
    print()
    if otherTransactions is not None:
        tx_parts.append(otherTransactions)
    return pd.concat(tx_parts, ignore_index=True)

account_files = glob.glob(account_file + '*')
accountFileRanges = getAccountFileRanges(account_files)
otc_transactions = getOTCTrades(otc_file)
txa = combineTransactions(accountFileRanges, otc_transactions)
# print(txa)
#txa=readAccountFile(account_file)

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


pd.options.mode.chained_assignment = None  # default=='warn' # TODO: Fix this
tx['Symbol'] = tx['Instrument']
setInstrument(tx)

splits_list = []
def addSplit(poss_split_date, symbol, poss_split_ratio):
    split = pd.DataFrame(data={'Date': poss_split_date, "Symbol": symbol, "Ratio": poss_split_ratio}, index=[0])
    splits_list.append(split)

def calculateSplitInfo():
    per_symbol = tx.groupby('Symbol')
    for symbol, transactions in per_symbol:
        tx_iter = transactions.iterrows()
        last_price = next(tx_iter)[1]['Price']
        for _, row in tx_iter:
            if last_price / row['Price'] > 1.75:
                poss_split_ratio = round(last_price / row['Price'])
                print("Possible split in {} of 1:{} before {}".format(symbol, poss_split_ratio, getFormattedDate(row['Date'])))
                poss_split_date = row['Date'] - pd.Timedelta(days=1)
                addSplit(poss_split_date, symbol, poss_split_ratio)
            last_price = row['Price']
    print()

def writeSplitInfoToFile(si_file):
    if splits_list:
        pd.concat(splits_list, ignore_index=True).to_csv(si_file, index=False)
    if not path.exists(split_info_file):
        copyfile(si_file, split_info_file)
        print("Split info file {} not found. Copying {}... Please inspect the split info file for any errors.".format(
            split_info_file, si_file))

if not skip_split_info_calculation:
    calculateSplitInfo()
    writeSplitInfoToFile(generated_split_info_file)


splits = pd.read_csv(split_info_file)
# splits['Date'] = pd.to_datetime(splits['Date'], format='%Y-%m-%d')

for _, split in splits.iterrows():
    s = tx[(tx['Symbol'] == split['Symbol']) & (tx['Date'] <= split['Date'])]
    s['No. of Shares'] = s['No. of Shares'] * split['Ratio']
    s['Price'] = s['Price'] / split['Ratio']
    tx.update(s)

qty = tx['No. of Shares'] * tx['Amount'].transform(np.sign)
tx = tx.copy()
tx['Qty'] = qty

tx_filtered = tx
# tx_filtered = tx_filtered[tx_filtered['Date'] > np.datetime64('today') - pd.Timedelta(weeks=1)]
# tx_filtered = tx_filtered[tx_filtered['Date'] < np.datetime64('today') - pd.Timedelta(weeks=1)]
# tx_filtered = tx_filtered[tx_filtered['Date'] < np.datetime64('today') - pd.Timedelta(days=1)]
# tx_filtered = tx_filtered[tx_filtered['Date'] < np.datetime64('2021-02-01')]
if tx_filter_end_date:
    print("Filtering out transactions made after : {}".format(tx_filter_end_date))
    tx_filtered = tx_filtered[tx_filtered['Date'] <= tx_filter_end_date]
if tx_filter_start_date:
    print("Filtering out transactions made before : {}".format(tx_filter_start_date))
    tx_filtered = tx_filtered[tx_filtered['Date'] >= tx_filter_start_date]
if no_buy_after_date:
    print("Filtering out buy transactions made after : {}".format(no_buy_after_date))
    tx_filtered = tx_filtered[(tx_filtered['Date'] <= no_buy_after_date) | (tx_filtered['Qty'] <= 0)]

qty_amount = tx_filtered[['Instrument', 'Qty', 'Amount']].groupby('Instrument', as_index=False).sum()
qty_amount['PPS'] = qty_amount['Amount'] / qty_amount['Qty']

last_traded_price = (tx_filtered if ignore_filtered_out_transactions_for_price else tx)[['Instrument', 'Price']].groupby('Instrument').last().to_dict()['Price']

pf = None
pf_price = {}
qty_mismatched_symbols = []

if path.exists(pf_file):
    pf = pd.read_excel(pf_file, header=2, parse_dates=True)
    pf = pf.dropna()
    pf['Symbol'] = pf['Security'].str.extract(r'(?P<Instrument>[A-Z.]*)')
    pf['Sell Price'] = pf['Sales Proceeds'] / pf['Quantity']
    setInstrument(pf)
    pf_price = pf[['Instrument', 'Traded Price']].groupby('Instrument').last().to_dict()['Traded Price']
    sales_commission = (pf['Traded Price'] / pf['Sell Price'])[0]
    if not skip_split_info_calculation:
        qty_amount_ins = qty_amount
        qty_amount_ins['Symbol'] = qty_amount_ins['Instrument'].map(lambda i: mapped_symbol[i])
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        #     print(pf)
        qty_amount_pf = pd.merge(qty_amount_ins, pf, on=['Symbol'])
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print(qty_amount_pf)
        qty_amount_pf = pd.merge(qty_amount_ins, pf, on=['Symbol'], validate="1:1")
        pf_quantities = qty_amount_pf[['Symbol', 'Qty', 'Quantity']]
        mismatched_pf_quantities = pf_quantities[pf_quantities['Qty'] != pf_quantities['Quantity']]
        for _, row in mismatched_pf_quantities.iterrows():
            print("Quantity mismatch in {}. Possible misconfigured split or scrip dividend - Quantities are {} vs {}".format(row['Symbol'],
                                                                                                        row['Qty'],
                                                                                                        row['Quantity']))
            poss_split_ratio_approx = row['Quantity'] / (row['Qty'] if row['Qty'] !=0 else 1)
            if (poss_split_ratio_approx == round(poss_split_ratio_approx)):
                last_date = tx[tx['Symbol'] == row['Symbol']].groupby('Symbol', as_index=False).last()['Date'][0]
                print("Possible recent split of 1:{} in {} after {}".format(round(poss_split_ratio_approx), row['Symbol'],
                                                                            last_date))
                addSplit(last_date, row['Symbol'], round(poss_split_ratio_approx))
            qty_mismatched_symbols.append(row['Symbol'])
        writeSplitInfoToFile(generated_poss_split_info_file)
    txp = tx
    txp['Approx. share value'] = txp['Amount'] / txp['Qty']
    if qty_mismatched_symbols:
        for mmi in qty_mismatched_symbols:
            if print_qty_mismatched_symbol_info:
                print("\nMismatched Symbol : {}".format(mmi))
                print(pf[pf['Symbol'] == mmi].to_string())
                print(qty_amount[qty_amount['Symbol'] == mmi].to_string())
                print(txp[txp['Symbol'] == mmi].to_string())
                print("\n")
        if not print_qty_mismatched_symbol_info:
            print("Quantity mismatches found. Run with --print_qty_mismatched_symbol_info to get additional information.\n")
    else:
        print("No quantity mismatches found. Split info is likely up to date.\n")

    if not skip_quotes_file_generation:
        # Dump Quotes file to match the Yahoo Finance import format
        # pff=pd.DataFrame()
        # pff['Symbol']=tx['Symbol'].map(lambda s: s.replace('.','') + '0000.CM')
        # pff['Date']=tx['Date'].dt.strftime('%Y/%m/%d')
        # pff['Time']='14:29 IST'
        # pff['Trade Date']=tx['Date'].dt.strftime('%Y%m%d')
        # pff['Purchase Price']=tx['Price']
        pff = pd.DataFrame()
        pff['Symbol'] = pf['Security'].map(lambda s: s.replace('.', '') + '.CM')
        pff['Date'] = datetime.today().strftime('%Y/%m/%d')
        pff['Time'] = '14:29 IST'
        pff['Trade Date'] = datetime.today().strftime('%Y%m%d')
        pff['Purchase Price'] = pf['Avg Price']
        pff['Quantity'] = pf['Quantity']
        pff['Commission'] = (pf['B.E.S Price'] - pf['Avg Price']) * pf['Quantity']
        pff.sort_values('Commission').to_csv("Quotes.csv")
else:
    print("\nPortfolio file {} does not exist. Save the Portfolio file as well for more accurate results\n".format(
        pf_file))

last_price =  {**last_traded_price} if ignore_portfolio_last_price else {**last_traded_price, **pf_price}

tx_filtered['Eff. Price'] = tx_filtered['Amount'] / tx_filtered['Qty']
tx_filtered['Last Price'] = tx_filtered['Instrument'].map(lambda i: last_price[i])
tx_filtered['Last Price - Effective'] = tx_filtered['Last Price'] * tx_filtered['Amount'].map(
    lambda a: sales_commission if a >= 0 else 2 - sales_commission)

def buySellAggregate(groupby):
    bs = groupby.agg(
        {'Qty': 'sum', 'Amount': 'sum', 'Last Price': 'last', 'Last Price - Effective': 'last'})
    bs.insert(2, 'Avg. Price', bs['Amount'] / bs['Qty'])
    return bs

if day_reports:
    print("Day Reports:")
    day_report = buySellAggregate(tx_filtered[tx_filtered['Date'].isin(day_reports)].groupby(['Date', 'Instrument', 'TX_Type']))
    print(day_report.to_string())

tx_filtered_bs = buySellAggregate(tx_filtered.groupby(['Instrument', 'TX_Type']))
print(tx_filtered_bs.to_string())
print()

def depositFilter(txa):
    return txa['Transaction Type'] == "R"

def withdrawalFilter(txa):
    return txa['Transaction Type'] == "PV"

def getBalanceChangeSummary(txa):
    balanceChanges = txa[(depositFilter(txa) | withdrawalFilter(txa))][["Date", "Amount"]].sort_values(by="Date")
    balanceChanges["Amount"] = - balanceChanges["Amount"]
    balanceChanges["Balance"] = balanceChanges["Amount"].cumsum()
    return balanceChanges

def removeBalanceSummaryWithdrawals(balanceChanges):
    total_withdrawals = - balanceChanges[balanceChanges["Amount"] < 0]["Amount"].sum()
    deposits = balanceChanges[balanceChanges["Amount"] > 0]
    for i, row in deposits.iterrows():
        if total_withdrawals > row["Amount"]:
            total_withdrawals -= row["Amount"]
            deposits.drop(i, inplace=True)
        else:
            deposits.at[i, "Amount"] = row["Amount"] - total_withdrawals
            break
    return deposits

def equalPeriodBalance(balanceChanges, last_balance, last_date, period = 'M'):
    # remainder = last_balance - balanceChanges["Balance"].values[-1]
    balanceChanges = balanceChanges.append({"Date":last_date, "Amount":-last_balance, "Balance": 0}, ignore_index=True)

    balanceChanges["Period"] = balanceChanges["Date"].dt.to_period(period)
    periodSum = balanceChanges[["Period", "Amount"]].groupby("Period").sum().reset_index()

    drange = pd.date_range(start=periodSum["Period"].min().to_timestamp(), end=periodSum["Period"].max().to_timestamp() , freq=period)
    seriesDict = {}
    for p in drange:
        seriesDict[p.to_period(period)] = 0
    for _, row in periodSum.iterrows():
        seriesDict[row["Period"]] = row["Amount"]
    periodBalances =  pd.DataFrame([{"Period" : k, "Amount": v} for k, v in seriesDict.items()])

    return periodBalances

def getIRR(periodBalances):
    # return np.irr(- periodBalances["Amount"])
    return round(numpy_financial.irr(- periodBalances["Amount"]), 5)

def toPctString(v):
    return '{:.2%}'.format(v)

def balanceChangeSummaryToPct(balanceChanges, pct = 1):
    balanceChangesPct = balanceChanges[["Date"]]
    balanceChangesPct["Amount %"] =  (balanceChanges["Amount"]  / pct).map(lambda v: ("" if v <0 else "+") + toPctString(v))
    balanceChangesPct["Balance %"] = (balanceChanges["Balance"] / pct).map(lambda v: toPctString(v))
    balanceChangesPct.style.format({
        'Amount %': '{:.2%}'.format,
        'Balance %': '{:.2%}'.format,
    })
    return balanceChangesPct

# Duplicate Code
def periodBalancesToPct(periodBalances, pct = 1):
    periodBalancesPct = periodBalances[["Period"]]
    periodBalancesPct["Amount %"] =  (periodBalances["Amount"] / pct).map(lambda v: ("" if v <0 else "+") + toPctString(v))
    periodBalancesPct.style.format({
        'Amount %': '{:.2%}'.format,
    })
    return periodBalancesPct

qty_amount['Last Price'] = qty_amount['Instrument'].map(lambda i: last_price[i])
qty_amount['Last Sell Price'] = qty_amount['Last Price'] / sales_commission
qty_amount['Current Value %'] = qty_amount['Last Sell Price'] / qty_amount['PPS'] * 100
qty_amount['Sales Proceeds'] = qty_amount['Last Sell Price'] * qty_amount['Qty']
qty_amount['Gain/Loss'] = qty_amount['Sales Proceeds'] - qty_amount['Amount']
print(qty_amount.sort_values('Gain/Loss', ascending=False).to_string(index=False))

total_purchased_amount = tx_filtered_bs[tx_filtered_bs['Amount'] >= 0]['Amount'].sum()
total_sold_amount = -tx_filtered_bs[tx_filtered_bs['Amount'] < 0]['Amount'].sum()

net_expense = qty_amount['Amount'].sum()
total_expense = net_expense + total_interest_paid
total_amount_transferred = -txa[depositFilter(txa)]['Amount'].sum()
total_amount_withdrawn = txa[withdrawalFilter(txa)]['Amount'].sum()
cash_balance = total_amount_transferred - total_expense - total_amount_withdrawn
pf_value = qty_amount['Sales Proceeds'].sum()
total_value = cash_balance + pf_value
total_gain_loss = qty_amount['Gain/Loss'].sum() - total_interest_paid
# assert(total_value - total_amount_transferred == total_gain_loss)
if abs((total_value - total_amount_transferred) - (total_gain_loss)) > 0.1:
    print("\nBalance mismatch: total_value - total_amount_transferred != total_gain_loss [ {0:,.2f} != {0:,.2f} ]".format(total_value - total_amount_transferred, total_gain_loss))

print("\nPortfolio Distribution Summary:")
diversity = qty_amount[['Instrument', 'PPS', 'Last Price']]
diversity['Gain/Loss %'] = 100 * (qty_amount['Gain/Loss']/ (qty_amount['Amount'].map(lambda p : p if p >=0 else 0)))
diversity['PF G/L %'] = 100 * (qty_amount['Gain/Loss']/ qty_amount['Gain/Loss'].sum())
diversity['PF % @ LastP'] = 100 * (qty_amount['Sales Proceeds'] / pf_value)
diversity['Cost % PF'] = 100 * (qty_amount['Amount'].map(lambda p : p if p >=0 else 0) / net_expense)
diversity['PF Rel Growth %'] = 100 * (diversity['PF % @ LastP'] / diversity['Cost % PF'])
diversity.style.format({
    'Gain/Loss %': '{:.2%}'.format,
    'PF G/L %': '{:.2%}'.format,
    'PF % @ LastP': '{:.2%}'.format,
    'Cost % PF': '{:.2%}'.format,
    'PF Rel Growth %': '{:.2%}'.format
})
diversity.sort_values('PF % @ LastP', ascending=False, inplace=True)
diversity.reset_index(drop=True, inplace=True)
diversity['Total %'] = diversity['PF % @ LastP'].cumsum()
diversity['PPS'] = diversity['PPS'].map(lambda p : p if p >=0 else "Negative")
print(diversity.to_string(index=True))

print ("\nBalance change history:")
balanceChanges = getBalanceChangeSummary(txa)
print(balanceChanges.to_string(index=False))
monthlyPeriodBalances = equalPeriodBalance(balanceChanges, total_value, txa["Date"].max())
print(monthlyPeriodBalances.to_string(index=False))
print(balanceChangeSummaryToPct(balanceChanges, total_value).to_string(index=False))
print(periodBalancesToPct(monthlyPeriodBalances, total_value).to_string(index=False))
monthly_irr=getIRR(monthlyPeriodBalances)

print()

print("Total Purchase Amount: {0:,.2f}".format(total_purchased_amount))
print("Total Sold Amount: {0:,.2f}".format(total_sold_amount))
print(
    "Total Commission Approx. : {0:,.2f}".format((sales_commission - 1) * (total_purchased_amount + total_sold_amount)))

print("Net cost of portfolio: {0:,.2f}".format(net_expense))
print("Total interest paid: {0:,.2f}".format(total_interest_paid))
print("Total expense for the portfolio: {0:,.2f}".format(total_expense))
print("Total deposits: {0:,.2f}".format(total_amount_transferred))
print("Total withdrawals: {0:,.2f}".format(total_amount_withdrawn))
print("Current Portfolio value: {0:,.2f}".format(pf_value))
print("Cash balance: {0:,.2f}".format(cash_balance))
print("Total value: {0:,.2f}".format(total_value))
print("Total Gain/Loss: {0:,.2f}".format(total_gain_loss))
print("Total Gain/Loss %: {0:,.2f}%".format(total_gain_loss/total_amount_transferred * 100))
print("Fully Liquidated Monthly IRR %: {:.2%}".format(monthly_irr))
print("Fully Liquidated Annualized IRR %: {:.2%}".format((1 + monthly_irr) ** 12 - 1))
# print(qty_amount.to_string())
