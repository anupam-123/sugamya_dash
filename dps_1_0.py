""" Data Preparation Sub-system (utils) of Sugamya/CodeSamarthya/GOS/DASH

This script works with data that is manually loaded onto a local working
machine.

NOTE: Any changes made in this docstring need to be reflected in the
 appropriate README.md files.

This module consists of the following parts:

    * Loading Data from Datasets folder
        - requires manual entry for change of files
    * Cleaning & Basic pre-processing of Data
    * Validating Data

Datasets
--------
Apart from the names of the .csv files, following datasets are unaltered and
directly downloaded from respective sources:

    * front desk data from Notion
    * PayTM settlements & transactions
    * Booking.com
    * InGo-MMT

The following has been altered with only account credits mentioned:
    * Bank transactions

Authors
-------
Anupam Neelavar (anupam2neelavar@gmail.com)
Pooja Bhagavat Memorial Mahajana Education Centre

Gaurav S Hegde (grv.hegde@gmail.com)
Sugamya/CodeSamarthya
"""

## Import necessary libraries
# Built-ins
import os
import sys
import itertools
import datetime

from argparse import ArgumentParser

# Third party packages
import pandas as pd
import numpy as np

# user-defined modules
import dps_utils as utils



# Set few global options for Pandas ...
# Display pd.DataFrame objects extensively
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)

# Constants used in the rest of the script
const = {
    # Total amount inclding GST
    'BCOM_GST': 1.12,
    # Booking.com Commission
    'BCOM_COMMISSION': 0.18,
    # MMT Commission
    'MMT_COMMISSION': 0.77,
    # GST on room bill
    'ROOM_TAX': 0.12,
    # Tax on Extra Person Charges
    'EXTRA_PERSON_TAX': 0.12,
    # Tax on Extra services (??)
    'EXTRA_SERVICES_TAX': 0.12,
}

# Default paths to the directories holding data
dir_paths_default = {
    # Path to front desk data
    'FRONT_PATH': './dps_in/Front-Desk/',
    # Path to bank statement data
    'BNK_PATH': './dps_in/Bank-Statement/',
    # Path to PayTM transactions data
    'PTM_SET_PATH': './dps_in/PayTM/settlements',
    # Path to PayTM settlements data
    'PTM_TRANS_PATH': './dps_in/PayTM/transactions/',
    # Path to booking.com reservations data
    'BK_COM_PATH': './dps_in/OTA/Booking-com/',
    # Path to InGo-MMT data
    'INGO_PATH': './dps_in/OTA/InGo-MMT/'
}


# Creating an object of ArgumentParser class
args = ArgumentParser()

### Parsing command line arguments passed while running script
args.add_argument('-kk', '--no_file', type=int, dest='file_no',
                  help='Enter number of files to be processed')
args.add_argument('-d', '--default_path', dest='diff_path',
                  action='store_true', 
                  default=False, help='flag to use default path')

# Arguments for front-desk dataset
args.add_argument('-f', '--frontdesk', type=str, dest='fd_path',
                  default=dir_paths_default['FRONT_PATH'],
                  help='Path to for front desk data')

# Arguments for paytm settlement and transaction dataset
args.add_argument('-ps', '--paytm_settlements', type=str, dest='ptm_s_path',
                  default=dir_paths_default['PTM_SET_PATH'],
                  help='Path to for paytm settlement data')

args.add_argument('-pt', '--paytm_transactions', type=str, dest='ptm_t_path',
                  default=dir_paths_default['PTM_TRANS_PATH'],
                  help='Path to for paytm transaction data')

# Arguments for booking.com dataset
args.add_argument('-b', '--bank_statement', type=str, dest='bank_path',
                  default=dir_paths_default['BNK_PATH'],
                  help='Path to for bank statement')

# Arguments for bank statement dataset
args.add_argument('-bk', '--booking_com', dest='bcom_path',
                  default=dir_paths_default['BK_COM_PATH'],
                  type=str, help='Path to for front-desk data')

# Arguments for OTA:InGo-MMT dataset
args.add_argument('-om', '--ota_mmt', type=str, dest='mmt_path',
                  default=dir_paths_default['INGO_PATH'],
                  help='Path to for ota data')

# Create the object for parse_args
PARSER = args.parse_args()


# Create a list of all the paths from the command line arguments
##### Turn this into dictionary

file_path = [PARSER.fd_path, PARSER.ptm_s_path, PARSER.ptm_t_path,
             PARSER.bank_path, PARSER.bcom_path, PARSER.mmt_path]

if False in [os.path.exists(i) for i in file_path]:
    sys.stdout.write("Please check you default path or enter correct path for \
                     all the files")
    sys.exit(0)

#### Logic to input the default path or custom path 
#### w.r.t to command line arguments.
if PARSER.diff_path:
    for i in file_path:
        if i is False:
            fd_frame = utils.fd_data(dir_paths_default['FRONT_PATH'],
                                     PARSER.file_no)
        else:
            fd_frame = utils.fd_data(PARSER.fd_path, PARSER.file_no)
        if i is False:
            ptm_settle = utils.ptm_settle(dir_paths_default['PTM_SET_PATH'],
                                          PARSER.file_no)
        else:
            ptm_settle = utils.ptm_settle(PARSER.ptm_s_path, PARSER.file_no)
        if i is False:
            ptm_trans = utils.ptm_trans(dir_paths_default['PTM_TRANS_PATH'],
                                        PARSER.file_no)
        else:
            ptm_trans = utils.ptm_trans(PARSER.ptm_t_path, PARSER.file_no)
        if i is False:
            bcom = utils.bc_data(dir_paths_default['BK_COM_PATH'],
                                 PARSER.file_no)
        else:
            bcom = utils.bc_data(PARSER.bcom_path, PARSER.file_no)
        if i is False:
            ingommt = utils.ingo_mmt_data(dir_paths_default['INGO_PATH'],
                                          PARSER.file_no)
        else:
            ingommt = utils.ingo_mmt_data(PARSER.mmt_path, PARSER.file_no)
        if i is False:
            bnk_state = utils.bnk_state(dir_paths_default['BNK_PATH'],
                                        PARSER.file_no)
        else:
            bnk_state = utils.bnk_state(PARSER.bank_path, PARSER.file_no)
        break

# Split the column "Date" into two columns "check-in" and "check-out"
fd_frame[["check-in", "check-out"]] \
    = fd_frame["Date"].str.split("→", expand=True).fillna(0)

# Cnvert the column "check-in" and "check-out" into datetime format
fd_frame["check-in"] = fd_frame["check-in"].apply(utils.dt_conv_mix)
fd_frame["check-out"] = fd_frame["check-out"].apply(utils.dt_conv_mix)

# Find the difference between "check-in" and "check-out"
diff_date = fd_frame["check-out"] - fd_frame["check-in"]

# Fetch the number of days from the difference of "check-in" and "check-out"
fd_frame["Nights"] = diff_date.apply(lambda x: x.days).astype("int32")

# Capitalize all text in the DataFrame
fd_frame = fd_frame.applymap(lambda x: x.upper() if isinstance(x, str) else x)

# From UPI Details column, extract the UPI transaction date and amount
fd_frame["upi_transaction_date"] = fd_frame["UPI Details"].apply(
    lambda x: utils.upi_date_time(x)[0])

# From UPI Details column, extract the UPI transaction amount
fd_frame["upi_trans_amt"] = fd_frame["UPI Details"].apply(
    lambda x: utils.upi_date_time(x)[1])

# Get maximum length of the list in the column "upi_transaction_date"
len_upi_dt = int(fd_frame["upi_transaction_date"].str.len().max())

# Fill the empty list position with 0
fd_frame["upi_transaction_date"] = fd_frame["upi_transaction_date"].apply(
    lambda x: x + [0] * (len_upi_dt - len(x)) if isinstance(x, list) else [])

# Convert the date in the column "upi_transaction_date" into datetime format
# else fill the empty list position with NaT
fd_frame["upi_transaction_date"] = fd_frame["upi_transaction_date"].apply(
    lambda x: [pd.to_datetime(element, format="%d%m%Y", errors="coerce").date()
               if element else pd.NaT for element in x])

# Convert elements inside the list from string to integer.
fd_frame["upi_trans_amt"] = fd_frame["upi_trans_amt"].apply(lambda x: [
    int(element) if isinstance(x, list) and element and element.isdigit()
    else 0 for element in x])

# Get maximum length of the list in the column "upi_trans_amt"
max_ele_len_amt = int(fd_frame["upi_trans_amt"].str.len().max())

# Fill the empty list position with 0
fd_frame["upi_trans_amt"] = fd_frame["upi_trans_amt"].apply(
    lambda x: x + [0] * (max_ele_len_amt - len(x)) if isinstance(x, list)
    else [])

# Drop the column "Date" from the front-desk dataset
fd_frame = fd_frame.drop(columns="Date")

# Drop columns with all null values
ptm_trans_dp = ptm_trans.dropna(axis=1)

# Remove " ' " from the column "ptm_trans" dataframe
ptm_trans_dp = ptm_trans_dp.replace("'", "", regex=True)

# Drop columns with all null columns from paytm settlement dataset
ptm_settle = ptm_settle.dropna(axis=1)

# Drop columns containing the specified attributes
ptm_settle = ptm_settle.drop(columns=["Response_code", "Response_message",
                                      "Prepaid_Card", "Bank/Gateway",
                                      "Product_Code", "Bank_Transaction_ID",
                                      "Channel", "Transaction_Type", "MID"])

# Remove " ' " from the column "ptm_settle" dataframe
ptm_settle = ptm_settle.replace("'", "", regex=True)

# Merging paytm Transaction and settlement data
ptm_dataset = pd.merge(ptm_settle, ptm_trans_dp, on="UTR_No.", how="outer",
                       suffixes=("_settlement", "_transaction"))

# Convert the column "Transaction_Date_transaction" into datetime format
ptm_dataset["ptm_trans_date"] = pd.to_datetime(
    ptm_dataset["Transaction_Date_transaction"]).dt.date

# Convert the column "ptm_trans_date" into datetime format
ptm_dataset["ptm_trans_date"] = pd.to_datetime(ptm_dataset["ptm_trans_date"]
                                               ).dt.date

# Removal outlier dataset of PayTM Transaction
incon_trans_data = ptm_dataset[~ptm_dataset["UTR_No."]
                               .isin(ptm_settle["UTR_No."])]

# Inconsistent paytm dataset not matched with paytm settlement dataset
incon_settle_data = ptm_dataset[
    ptm_dataset["UTR_No."].isin(ptm_settle["UTR_No."])]

# Paytm dataset clean of unmatched or inconsistent data points
ptm_data_consi = incon_settle_data[incon_settle_data["UTR_No."]
                                   .isin(ptm_trans_dp["UTR_No."])] \
                                   .reset_index(drop=True)

# Drop columns with all null values
bnk_state = bnk_state.dropna(axis=1, how="all")

# Reset the index of the bnk_state dataframe
bnk_state = bnk_state.reset_index().drop(columns="index")

# Convert the column "Value Date" into datetime format and rename it to
# "trans_posval_date"
bnk_state["trans_posval_date"] = pd.to_datetime(bnk_state["Value Date"])

# Drop the column "Value Date"
bnk_state = bnk_state.drop(columns=["Value Date"], axis=0)

# Convert datetime only to time format
bnk_state["trans_post_time"] = pd.to_datetime(bnk_state[
    "Transaction Posted Date"], format="%d-%m-%Y %I:%M:%S %p",).dt.time

# Drop the column "Transaction Posted Date"
bnk_state = bnk_state.drop(columns=["Transaction Posted Date"])

# Extract reference number from the column "Transaction Remarks"
bnk_state["ref_no"] = (bnk_state["Transaction Remarks"].str.replace("-", "|")
                       .str.replace("/", "|").str.split("|").str[1])


# Drop columns with all null values
mmt_dataset = ingommt.drop(columns=["Total Recovered", "Recoveries Made",
                                    "Recoveries PNR", "Recoveries Date",
                                    "Recoveries Type", "Amount Paid in Bank",
                                    "Payments Made in Bank Account"])


# Drop redundant columns and useless columns
bc_df = bcom.drop(columns=["Guest Name(s)", "Booker group", "Payment Method"])

# Group only records with status as "ok"
bc_df = bc_df[bc_df["Status"] == "ok"].reset_index(drop="index")

# Calculate the price with GST
bc_df["price_gst"] = round(bc_df["Price"] * const['BCOM_GST'])

# Match front_desk_data with paytm dataset
fd_dataset = []


#### Generate datasets by matching front_desk_dataset and paytm data set
for index in range(0, len_upi_dt):
    (fr_amt_lst, pay_dt_lst, ptm_trans_id, fr_name, ph_no, adults, mode_book,
     rm_book, checkin, checkout, room_bill, child, fd_idx, paid_checkin,
     total_amt_paid, paid_checkout, advance_paid, adv_pay_met, checkin_mtd,
     checkout_mtd, extras_paid, ext_pay_met, extra_per_chrg) \
        = ([], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
           [], [], [], [], [], [])

    # forms upi transaction amount based on index
    fd_upi_idx = fd_frame.index[np.where(fd_frame["upi_trans_amt"]
                                         .apply(lambda x: x[index]
                                         if True
                                         and len(x) > index
                                         and x[index] > 0
                                         else False))]

    # loop for matching  front desk dataset, paytm datasets
    for i in fd_upi_idx:
        for j in range(1, len(ptm_data_consi["ptm_trans_date"])):
            if (fd_frame.iloc[i]["upi_transaction_date"][index]
                == ptm_data_consi["ptm_trans_date"][j]) \
                and (fd_frame.iloc[i]["upi_trans_amt"][index]
                     == ptm_data_consi["Amount_transaction"][j]):
                fr_amt_lst.append(fd_frame.iloc[i]["upi_trans_amt"][index])
                pay_dt_lst.append(ptm_data_consi["ptm_trans_date"][j])
                fr_name.append(fd_frame.iloc[i]["Name"])
                ptm_trans_id.append(ptm_data_consi[
                    "Transaction_ID_transaction"][j])
                ph_no.append(fd_frame.iloc[i]["Phone"])
                adults.append(fd_frame.iloc[i]["Adults"])
                mode_book.append(fd_frame.iloc[i]["Mode of Booking"])
                rm_book.append(fd_frame.iloc[i]["Rooms Booked"])
                checkin.append(fd_frame.iloc[i]["check-in"])
                checkout.append(fd_frame.iloc[i]["check-out"])
                room_bill.append(fd_frame.iloc[i]["Room Bill (Incl. GST)"])
                child.append(fd_frame.iloc[i]["Children"])
                fd_idx.append(i)
                paid_checkin.append(fd_frame.iloc[i]["Paid at Check-in"])
                paid_checkout.append(fd_frame.iloc[i]["Paid at Check-out"])
                total_amt_paid.append(fd_frame.iloc[i]["Total Amount Paid"])
                advance_paid.append(fd_frame.iloc[i]["Advance Paid"])
                adv_pay_met.append(fd_frame.iloc[i]["Advance Payment Method"])
                checkin_mtd.append(fd_frame.iloc[i]["Check-in Payment Method"])
                checkout_mtd.append(
                    fd_frame.iloc[i]["Check-out Payment Method"])
                extras_paid.append(fd_frame.iloc[i]["Extras Paid"])
                ext_pay_met.append(fd_frame.iloc[i]["Extras Payment Method"])
                extra_per_chrg.append(fd_frame.iloc[i][
                    "Extra Person Charges (Incl. GST)"])
        fd_cp_dt = pd.DataFrame(list(zip(fr_amt_lst, pay_dt_lst, ptm_trans_id,
                                         fr_name, ph_no, adults, mode_book,
                                         rm_book, checkin, checkout, room_bill,
                                         child, fd_idx, paid_checkin,
                                         checkin_mtd, paid_checkout,
                                         checkout_mtd, total_amt_paid,
                                         advance_paid, adv_pay_met,
                                         extras_paid, ext_pay_met,
                                         extra_per_chrg)),
                                columns=["Amount", "Amount_date",
                                         "Bank_Transaction_ID", "guest_name",
                                         "ph_no", "adults", "book_mode",
                                         "room_booking", "check-in",
                                         "check-out", "Room_Bill", "child",
                                         "Row_Id", "paid_checkin",
                                         "checkin_mtd", "paid_checkout",
                                         "checkout_mtd", "total_amt_paid",
                                         "advance_paid", "adv_pay_met",
                                         "extras_paid", "ext_pay_met",
                                         "extra_per_chrg"])
    fd_dataset.append(fd_cp_dt)
fr_dataset = pd.concat(fd_dataset).reset_index(drop=True)
fr_edit = fr_dataset[fr_dataset["Row_Id"].duplicated(keep=False)] \
                .sort_values(by="Row_Id").groupby("Row_Id")['Amount'].sum()
fr_unique = fr_dataset[~fr_dataset["Row_Id"].duplicated(keep=False)]

fd_str_num = fr_dataset["room_booking"].str.len()

# extract only row which status "ok" e.g, All transaction is complete
# b/w customer and guest house
bc_idx = bc_df[bc_df["Status"] == "ok"].index

# Extract rows where the customers have made booking through
# OTA booking.com
fd_comp_idx = fr_dataset[fr_dataset["book_mode"] == "BOOKING.COM"].index


# Match the front-desk data with booking.com data and append the
# booking.com
for p in fd_comp_idx:
    for y in bc_idx:
        if p < len(fr_dataset) and y < len(bc_df):
            if (fr_dataset.iloc[p]["check-in"] == bc_df.iloc[y]["Check-in"]
                and fr_dataset.iloc[p]["check-out"] == bc_df.iloc[y][
                    "Check-out"]
                and fd_str_num[p] == bc_df.iloc[y]["Rooms"]
                and utils.trigram_bool(fr_dataset.iloc[p]["guest_name"],
                                       bc_df.iloc[y]["Booked by"], 0.2)):
                fr_dataset.loc[p, "Booking_Id"] = bc_df["Book Number"][y]
                fr_dataset.loc[p, "price_gst"] = bc_df["price_gst"][y]
                fr_dataset.loc[p, "ota_commission_amount"] = round(
                    bc_df['Price'][y] * const['BCOM_COMMISSION'])


# Extract the paytm data if elements in "Transaction_ID_transaction" and
# "Bank_Transaction_ID" are matcting
fr_bnk_dataset = ptm_data_consi[ptm_data_consi["Transaction_ID_transaction"]
                                .isin(fr_dataset["Bank_Transaction_ID"])]

# Extract the bank statement data if elements in "ref_no" and "UTR_No."
# are matcting
fr_bc_bnk = bnk_state[bnk_state["ref_no"].isin(fr_bnk_dataset["UTR_No."])]

# Match the front-desk data with bank statement and paytm dataset,
# and append the bank transaction id
for i in fr_bnk_dataset.index:
    for k in fr_dataset.index:
        for j in fr_bc_bnk.index:
            if (fr_bnk_dataset["UTR_No."][i] == fr_bc_bnk["ref_no"][j]) \
                and (fr_dataset["Bank_Transaction_ID"][k]
                     == fr_bnk_dataset["Transaction_ID_transaction"][i]):
                
                # Append the bank transaction id to the dataset
                fr_dataset.loc[k, "Tran_Id"] = fr_bc_bnk["Tran. Id"][j]

# Combines the front-desk data, OTA:InGo-MMT and bank statement data
(fd_idx, bk_id, name, checkin, checkout, brand, ota_amt,
 bank_ref_no, trans_date, rooms, ph_no, adults, extra_per_chrg,
 total_amt_paid, extra_paid) \
    = ([], [], [], [], [], [], [], [], [], [], [], [], [], [], [])

# Only extract the confirmed booking from the OTA:InGo-MMT dataset
mmt_idx = mmt_dataset[mmt_dataset["Booking Status"] == "Confirmed"].index

# Match the front-desk data with OTA:InGo-MMT using features like
# "check-in", "check-out" and "Name"
for i in fd_frame.index:
    for j in mmt_idx:
        if ((fd_frame.iloc[i]["check-in"] == mmt_dataset.iloc[j][
             "Checkin Date"])
            and (fd_frame.iloc[i]["check-out"] == mmt_dataset.iloc[j][
                 "Checkout Date"])
            and utils.trigram_bool(fd_frame.iloc[i]["Name"],
                                   mmt_dataset.iloc[j]["Guest Name"],
                                   0.05)):
            fd_idx.append(i)
            bk_id.append(mmt_dataset.iloc[j]["Booking Id"])
            name.append(fd_frame.iloc[i]["Name"])
            checkin.append(fd_frame.iloc[i]["check-in"])
            checkout.append(fd_frame.iloc[i]["check-out"])
            brand.append(mmt_dataset.iloc[j]["Brand"])
            ota_amt.append(mmt_dataset.iloc[j]["Booking Amount"])
            bank_ref_no.append(mmt_dataset.iloc[j]["Bank Ref No"])
            trans_date.append(mmt_dataset.iloc[j]["Payments Date"])
            rooms.append(fd_frame.iloc[i]["Rooms Booked"])
            ph_no.append(fd_frame.iloc[i]["Phone"])
            adults.append(fd_frame.iloc[i]["Adults"])
            extra_per_chrg.append(
                fd_frame.iloc[i]["Extra Person Charges (Incl. GST)"])
            total_amt_paid.append(fd_frame.iloc[i]["Total Amount Paid"])
            extra_paid.append(fd_frame.iloc[i]["Extras Paid"])

fr_mmt_comb = pd.DataFrame(list(zip(fd_idx, ota_amt, trans_date,
                                    bk_id, name, checkin, checkout,
                                    brand, rooms, bank_ref_no, ph_no,
                                    adults, extra_per_chrg, total_amt_paid,
                                    extra_paid)),
                           columns=["Row_Id", "ota_amount", "trans_dt",
                                    "booking_Id", "cust_name", "checkin",
                                    "checkout", "brand", "room", "bank_ref_no",
                                    "ph_no", "Adults", "extra_per_charge",
                                    "total_amt_paid", "extra_paid"])

# Extract the bank statement data if elements in "ref_no" and "bank_ref_no"
# match
bnk_state_mmt = bnk_state[bnk_state["ref_no"].isin(fr_mmt_comb
                                                   ["bank_ref_no"])]

# Appends the bank transaction id to the front_desk dataset
for i in fr_mmt_comb.index:
    for j in bnk_state_mmt.index:
        if fr_mmt_comb["bank_ref_no"][i] == bnk_state_mmt["ref_no"][j]:
            fr_mmt_comb.loc[i, "trans_id"] = bnk_state_mmt["Tran. Id"][j]

# Calculate the commission amount for the OTA:InGo-MMT
fr_mmt_comb['ota_commission_amount'] = round((fr_mmt_comb['ota_amount'] -
                                              (fr_mmt_comb['ota_amount'] *
                                               const['MMT_COMMISSION'])))
# print(fr_mmt_comb.columns)
ls_data = []

column = ["Row_Id", "Room_Bill", "Amount_date", "Booking_Id", "guest_name",
          "check-in", "check-out", "book_mode", "room_booking", "ph_no",
          "adults", "extra_per_chrg", "total_amt_paid", "extras_paid",
          "Tran_Id", "ota_commission_amount"]

# Appends "columns" list series element to ls_data list
for i in column:
    ls_data.append(fr_dataset[i].tolist())
# print(ls_data)
# Loop to get all the columns from the front-office dataset
# except the bank_ref_no
mmt_list = [fr_mmt_comb[i].tolist() for i in fr_mmt_comb.columns if
            i != "bank_ref_no"]

# Convert seperate lists into a single list which
# contains sublist
for i, _ in enumerate(ls_data):
    for j, _ in enumerate(mmt_list):
        if i == j:
            ls_data[i].extend(mmt_list[j])

# Convert the list into DataFrame
data_ls = pd.DataFrame(list(zip(*ls_data)), columns=column)

ptm_trans = fr_dataset["Bank_Transaction_ID"].tolist()

# Convert "ptm_trans", "ph_no" into equal length
ex_len_diff = len(data_ls) - len(fr_dataset)
ptm_trans += [np.nan] * ex_len_diff

# Combine list to for "Data" DataFrame
data = pd.DataFrame(list(zip(*ls_data, ptm_trans)),
    columns=["Row_Id", "Room_Bill", "Amount_date", "Booking_Id",
             "guest_name", "checkin", "checkout", "Mode_of_Booking",
             "room_booking", "ph_no", "Adults", "extra_per_charge",
             "total_amt_paid", "extras_paid", 'Tran_Id',
             "ota_commission_amount", "Bank_Transaction_ID"])

# Sort the DataFrame by "Row_Id"
data = data.sort_values(by=["Row_Id"]).reset_index(drop=True)

col_list = ['Row_Id', 'guest_name', 'Room_Bill', 'checkin', 'checkout',
            'ph_no', 'Adults', 'Mode_of_Booking', 'total_amt_paid']

# aggregate the element in column names in col_list
ls_dt_a = data.groupby(col_list).agg(list).reset_index()

# Create a copy of the column "room_booking"
room = ls_dt_a.room_booking.copy()

# Create the subset of the ls_dt_a DataFrame
ls_dt_a = ls_dt_a[col_list]

# Group 'Tran_Id', 'Booking_Id', 'Bank_Transaction_ID' in to sublist
for iter_col in ['Tran_Id', 'Booking_Id', 'Bank_Transaction_ID', 'extras_paid',
                 "ota_commission_amount", 'extra_per_charge']:
    temp = data.groupby(col_list)[iter_col].agg(list).reset_index()
    ls_dt_a = pd.merge(ls_dt_a, temp, on=col_list, how='left')

# concatinate "ls_dt_a" and "room" to for front_office_complete
ls_dt_a = pd.concat([ls_dt_a, room], axis=1)

# Merge and remove the duplicates from the series in the column "room_booking
ls_dt_a.room_booking = ls_dt_a.room_booking.apply(utils.merge_list)

ls_dt_a_fin = ls_dt_a[['Row_Id', 'guest_name', 'Room_Bill', 'checkin',
                       'checkout', 'ph_no', 'Adults', 'Mode_of_Booking',
                       'Tran_Id', 'Booking_Id', 'Bank_Transaction_ID',
                       'room_booking']]


# Sort the DataFrame by "checkin"
fo_comp = ls_dt_a_fin.sort_values(by=["checkin"]).reset_index(drop=True)
fr_un = fr_edit.index.tolist()
fr_un.extend(fr_unique['Row_Id'].tolist())
fr_val = fr_edit.values.tolist()
fr_val.extend(fr_unique['Amount'].tolist())
fr_comp_val = pd.DataFrame(list(zip(fr_un, fr_val)), columns=['Row_Id',
                                                              'Amount'])
# print("fr_comp_val", fr_comp_val)

for i in fo_comp.Row_Id:
    for j in fr_comp_val.Row_Id:
        if j == i:
            fo_comp.loc[fo_comp['Row_Id'] == i, "paid_at_UPI"] = \
                fr_comp_val[fr_comp_val['Row_Id'] == j]['Amount'].values

# Reset the index of the DataFrame
fin_data = data.reset_index(level=0)

# Rename the column "index" to "front_office_index"
fin_data = fin_data.rename({"index": "Row_Id"}, axis="columns")

# Select specific columns from the financial_data DataFrame
fin_data = fin_data[["Tran_Id", "Bank_Transaction_ID",
                     "Booking_Id", "Mode_of_Booking",
                     "Row_Id"]]

fin_data_ = fin_data[fin_data.Tran_Id.isnull()==False] \
                    .reset_index(drop=True)

fin_data_ = fin_data_.groupby(['Tran_Id']).agg(list).reset_index()


# fetch unmatched front-desk data
fr_mani = fd_frame[~fd_frame.index.isin(ls_dt_a["Row_Id"])]

# Extract only cash transaction from the front-desk data
fo_cash = fr_mani[fr_mani["UPI Details"].apply(lambda x: len(x) == 0)]

# Remove the rows if it containing 'MMT', 'A/C', 'UPI', 'GOIBIBO'
fo_cash_ = fo_cash[~fo_cash.isin(['MMT', 'A/C', 'UPI', 'GOIBIBO']).any(axis=1)]

# Extract only unmatched UPI transaction from the front-desk data
fo_resi = fr_mani[fr_mani["UPI Details"].apply(lambda x: len(x) > 0)]

# Fetches row only contain 'MMT', 'A/C', 'UPI', 'GOIBIBO'
fo_resi_a = fo_cash[fo_cash.isin(['MMT', 'A/C', 'UPI', 'GOIBIBO']).any(axis=1)]

# concatinate "fo_resi" and "fo_resi_a" to for front_office_complete_residue
fo_resi = pd.concat([fo_resi, fo_resi_a], axis=0)

# Select specific columns from the fo_resi DataFrame
fo_resi = fo_resi[['Name', 'Room Bill (Incl. GST)', 'check-in', 'check-out',
                   'Phone', 'Adults', 'Mode of Booking', 'Children',
                   'Rooms Booked']]


def paid(method, col):
    """
    ...
    This function forms the front office dataset

    Parameters:
    -----------
    method: str
        The payment method e.g., "UPI", "CASH",
        "A/C", "CARD"
    col: str
        Name of the column e.g., "paid_upi", "paid_cash",
        "paid_act", "paid_card"

    Returns:
    -------
    pd.DataFrame
        The front-office dataset

    Examples:
    ---------
    >>> paid("UPI", "paid_upi")
    DataFrame
    """
    fin_trans = pd.DataFrame()

    for i, _ in fd_frame.iterrows():
        first, second, third, forth = 0, 0, 0, 0
        fin_trans.loc[i, "row_id"] = i
        fin_trans.loc[i, "room_bill"] \
            = fd_frame.iloc[i]["Room Bill (Incl. GST)"]
        fin_trans.loc[i, "total_amount_paid"] \
            = fd_frame.iloc[i]["Total Amount Paid"]
        fin_trans.loc[i, "paid_checkin"] \
            = fd_frame.iloc[i]["Paid at Check-in"]
        fin_trans.loc[i, "paid_checkout"] \
            = fd_frame.iloc[i]["Paid at Check-out"]
        fin_trans.loc[i, "paid_inbetween"] \
            = fd_frame.iloc[i]["Extras Paid"]
        if (fd_frame.iloc[i]["Check-in Payment Method"] 
                or fd_frame.iloc[i]["Check-out Payment Method"]
                or fd_frame.iloc[i]["Advance Payment Method"]
                or fd_frame.iloc[i]["Extras Payment Method"]) in method:
            if fd_frame.iloc[i]["Check-in Payment Method"] == method:
                first = fd_frame.iloc[i]["Paid at Check-in"]
            else:
                first = 0
            if fd_frame.iloc[i]["Check-out Payment Method"] == method:
                second = fd_frame.iloc[i]["Paid at Check-out"]
            else:
                second = 0
            if fd_frame.iloc[i]["Advance Payment Method"] == method:
                third = fd_frame.iloc[i]["Advance Paid"]
            else:
                third = 0
            if fd_frame.iloc[i]["Extras Payment Method"] == method:
                forth = fd_frame.iloc[i]["Extras Paid"]
            else:
                forth = 0
        if (fd_frame.iloc[i]["Mode of Booking"] == "BOOKING.COM"
                or fd_frame.iloc[i]["Mode of Booking"] == "MMT"
                or fd_frame.iloc[i]["Mode of Booking"] == "GOIBIBO"):
            fin_trans.loc[i, "paid_ota"] = (
                fd_frame.iloc[i]["Extra Person Charges (Incl. GST)"]
                + fd_frame.iloc[i]["Advance Paid"]
                + fd_frame.iloc[i]["Paid at Check-in"]
                + fd_frame.iloc[i]["Paid at Check-out"]
                + fd_frame.iloc[i]["Extras Paid"]
            )
        fin_trans.loc[i, col] = first + second + third + forth
    return fin_trans


# Append "UPI", "CASH", "A/C", "CARD" columns to the financial
# transaction dataset.
upi = paid("UPI", "paid_upi")
cash = paid("CASH", "paid_cash").iloc[:, -1]
acc = paid("A/C", "paid_act").iloc[:, -1]
card = paid("CARD", "paid_card").iloc[:, -1]
df_join = pd.concat([upi, cash, acc, card], axis=1)

### Generates the front-office dataset.
fr_office = pd.DataFrame(columns=["Row_Id", "guest_name", "ph_no",
                                  "adults", "child", "mode_of_booking",
                                  "checkin", "checkout"])
fr_ofc_room = pd.DataFrame(columns=["rooms"])
for i, _ in fd_frame.iterrows():
    fr_office.loc[i, "Row_Id"] = i
    fr_office.loc[i, "guest_name"] = fd_frame.iloc[i]["Name"]
    fr_office.loc[i, "ph_no"] = fd_frame.iloc[i]["Phone"]
    fr_office.loc[i, "adults"] = fd_frame.iloc[i]["Adults"]
    fr_office.loc[i, "child"] = fd_frame.iloc[i]["Children"]
    fr_office.loc[i, "mode_of_booking"] \
        = fd_frame.iloc[i]["Mode of Booking"]
    fr_office.loc[i, "checkin"] = fd_frame.iloc[i]["check-in"]
    fr_office.loc[i, "checkout"] = fd_frame.iloc[i]["check-out"]
    fr_ofc_room.loc[i, "rooms"] = fd_frame.loc[i]["Rooms Booked"]
fr_office = pd.concat([fr_office, fr_ofc_room], axis=1)


# Unmatched bank statement transactions
bnk_resi = bnk_state[~bnk_state['Tran. Id'].isin(fin_data_[
                     'Tran_Id'])]
# print(fin_data.columns)
# Combine bank transaction ID and front-desk index
bnk_match = fin_data[['Tran_Id', 'Row_Id']].copy()
# print("bnk_resi.columns", bnk_resi.columns)
# Concate Tran_Id and Tran_Id from bnk_match, bnk_resi respectively
bnk_match = pd.concat([bnk_match['Tran_Id'], bnk_resi['Tran. Id']],
                      axis=0).reset_index(drop=True)

# Rename 0 to Tran_Id
bnk_match = bnk_match.to_frame().rename(columns={0: 'Tran_Id'})

# Concate bnk_match and 'Row_Id' from financial_data to form
# bs_matching_table
bnk_match = pd.concat([bnk_match, fin_data['Row_Id']], axis=1)
bnk_match = bnk_match.groupby(['Tran_Id']).agg(list).reset_index()


bnk = pd.DataFrame(columns=['Bank_Transaction_ID', 'Row_ID', 'Booking_ID'])
bnk['Trans.Id'] = bnk_resi['Tran. Id']
bnk = bnk[['Trans.Id', 'Bank_Transaction_ID', 'Row_ID', 'Booking_ID']]

# print(df_bnk)
### Create csv file for front-office dataset and financial dataset.
fin_data_.to_csv(
    "./dps_out/AFS/bank_deposits_matched.csv", index=False)
# print(fin_data_.shape)
# Path to save 'bank_deposits_residue.csv' file
bnk.to_csv(
    "./dps_out/AFS/bank_deposits_residue.csv", index=False)

# Path to save 'front_office_complete_match.csv' file
fo_comp.to_csv(
    "./dps_out/AFS/front_office_match.csv", index=False)

# Path to save 'front_office_complete_residue.csv' file
fo_resi.to_csv(
    "./dps_out/AFS/front_office_residue.csv", index=False)

# Path to save 'front_office_cash.csv' file
fo_cash_.to_csv(
    "./dps_out/AFS/front_office_cash.csv", index=False)

# Path to save bank_deposits_matched.csv

# Path to save 'front_office.csv' file
df_join.to_csv(
    "./dps_out/VRS/front_office_full.csv", index=False)


# Path to save 'front_office.csv' file
fr_office.to_csv(
    "./dps_out/VRS/front_office.csv", index=False)

# Path to save 'bank_matching_table.csv' file
bnk_match.to_csv(
    "./dps_out/AFS/bs_fd.csv", index=False)

print("Successfull....!")