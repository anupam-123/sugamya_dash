"""This module contains wrapper functions, converter functions and function 
that combine different catagory file to single dataframe of different catagory.

 - The main function of this module is to convert the datatype, handle the
 missing values and convert the string to datetime.date.
   
 - This modules contains the function that matches the string from different 
 dataframe using trigram and returns the boolean value.
"""
##  third party module
import numpy as np
import pandas as pd

from fuzzy_match import algorithims

## inbuilt module
import glob
import os
import sys

# Global variables to skip the header and footer
# of the bank statement
SKIPHEAD = 16
SKIPFOOT = 38

## Functions useful for performing record linkages

def trigram_bool(first, second, threshold):
    """
    function to compare between two strings

    This function takes two strings and compare it using the 
    trigram algorithn and returns the boolean value based on the 
    threshold value.

    Parameters:
    ----------
    first: str
        The first string to be compared

    second: str
        The second string to be compared

    threshold: float
        The threshold between 0 and 1.

    Returns:
    -------
    bool
        The boolean value based on the threshold value.

    Examples:
    --------
    >>> trigram_bool('Athul', 'Athul Sasidharan', 0.5)
    True
    """
    if bool(algorithims.trigram(first, second) > threshold):
        return True
    else:
        return False

def merge_list(inp_list):
    """
    convert nested list to single list

    This function takes a list of lists or scalars as input and
    returns a new list that contains all the elements from the
    input list, without duplicates. If the input list contains
    any scalars, they are converted to zero before being added
    to the output list.

    ...

    Parameters:
    ----------
    inp_list: list
        list of lists or scalars to be merged.

    Returns:
    -------
    list
        A new list that contains all the elements from the input list, without
        duplicates.

    Examples:
    --------
    >>> merge_list([[1, 2, 3], [4, 5, 6]])
    [1, 2, 3, 4, 5, 6]

    >>> merge_list([[1, 2], [3, 4], 5])
    [0, 1, 2, 3, 4]

    >>> merge_list([[], [1], [1, 2]])
    [0, 1, 2]

    See Also:
    ---------
    dupli_list() : Removes the duplictes from the list
    """
    inp_list_ = set()
    for i in inp_list:
        if isinstance(i, list):
            for j in i:
                inp_list_.add(j)
        else:
            inp_list_.add(0)
    return list(inp_list_)

def dupli_list(x):
    """
    Removes the duplictes from the list
    
    ...

    This function takes the list converts the list into numpy array,
    converts the nan values to 0, Convert the data type to int and
    remove the duplicates by converting the array to set and then
    convert it back to list.

    Parameters:
    ----------
    x: list
        The list containing duplicates and nan values.

    Returns:
    -------
    list
        The list containing unique values.

    Examples:
    --------
    >>> dupli_list([1, 2, 3, 4, 5, 5, 5, 5, np.nan])
    [1, 2, 3, 4, 5]

    See Also:
    ---------
    merge_list() : convert nested list to single list
        
    """
    return list(set(np.nan_to_num(np.array(x), nan=0.0).astype(int)))


## Base function called by wrapper functions for loading data from various
## sources

def create_frame(folder_path, FILE_NO=0, conv=None, encode=None, skphead=0,
                 skpfoot=0):
    """
    Function takes the folder path and convert the datasets into
    dataframe.
    
    ...
    
    This function takes the following parameters and converts the datasets
    and converts it into respective dataframe by checking the extension of the
    file. If their are multiple file it concatinates dataframe with axis=0.

    Parameters:
    ----------
    folder_path: str
        The path of the file

    conv: dict
        The dictionary containing the converter functions

    encode: str, optional
        The encoding of the file.

    skphead: int, optional
        The number of rows to be skip.

    skpfoot: int, optional
        The number of rows to be skip from the bottom.

    Returns:
    -------
    DataFrame
        The dataframe containing manipulated data.

    Examples:
    --------
    >>> create_frame('path_to_file.csv', conv=conv, encode='utf-8')
    DataFrame
    """
    if isinstance(folder_path, str):
        file_list = glob.glob(os.path.join(folder_path, "*"))
  
    if FILE_NO is None:
        FILE_NO = len(file_list)

    if len(file_list) < FILE_NO:
        try:
            print(f"Insufficient files. {len(file_list)} are present in folder.\
                  Do you want to continue? (Y/N)")
            check = sys.stdin.readline().strip()
            if check == '':
                raise ValueError("Invalid Input")
            if check.upper() == "Y":
                pass
            if check.upper() == "N":
                raise SystemExit("Exiting the program")
        except ValueError as inp:
            print(inp)
        except SystemExit as inp:
            print(inp)
            sys.exit()
    
    conc_frame = pd.DataFrame()

    for _, folder_paths in enumerate(file_list[:FILE_NO]):
        print(folder_paths)
        spt = folder_paths.split(".")
        if spt[-1] == "csv":
            combframe = pd.read_csv(folder_paths, converters=conv,
                                    encoding=encode, skiprows=skphead,
                                    skipfooter=skpfoot)
        else:
            combframe = pd.read_excel(folder_paths, converters=conv,
                                      skiprows=skphead, skipfooter=skpfoot)
        conc_frame = pd.concat([conc_frame, combframe], axis=0,
                               ignore_index=True)

    return conc_frame


## Converter functions for corresponding wrapper functions for loading data
##from various sources

def int32_wrapper(x):
    """
    Data type conversion to int32

    This function convert data type to int32.

    Parameters:
    ----------
    x: int
       pass int

    Returns:
    -------
    int32 or np.nan

    Examples:
    >>> int32_wrapper(1)
    1
    """
    if x == "":
        return np.nan
    
    return np.int32(x)

def int64_wrapper(x):
    """
    Data type conversion to int64

       This function convert data type to int64.

    Parameters:
    ----------
    x: int
       pass int

    Returns:
    -------
    int64 or np.nan

    Examples:
    >>> int64_wrapper(1)
    1
    """
    if x == "":
        return np.nan
    
    return np.int64(x)

def float64_wrapper(x):
    """
    Data type conversion to float64

       This function convert data type to float64.

    Parameters:
    ----------
    x: int
       pass int

    Returns:
    -------
    float64 or np.nan

    Examples:
    >>> float64_wrapper(1.0)
    1.0
    """
    if x == "":
        return np.nan
    return np.float64(x)

def dt_conv(x):
    """
    convert datatime.date using specific format "%d-%m-%Y" 

        This function takes the date in string format and convert it
        into datetime.date format.

    Parameters:
    ----------
    x: str
       pass string

    Returns:
    -------
    datetime.date or pd.NaT

    Examples:
    >>> dt_conv('01-01-2021')
    datetime.date(2021, 1, 1)
    """
    if x == "":
        return pd.NaT
    return pd.to_datetime(x).date()

def conv_stamp_date(x):
    """
    Convert e.g. Timestamp to datetime

        This function takes the in the string format and convert it into
        datetime format.

    ...

    Parameters:
    ----------
    x: str
         pass string

    Returns:
    -------
    datetime or pd.NaT

    ...

    Examples:
    >>> conv_stamp_date(2021-01-01 00:00:00)
    datetime.date(2021, 1, 1)


    """
    if x == "":
        return pd.NaT

    return pd.to_datetime(x)

def dt_conv_mix(x):
    """ convert datatime.date using specific format "mixed" 

        This function takes the date in string format and convert it into
        datetime.date format.

    Parameters:
    ----------
    x: str
         pass string

    Returns:
    -------
    datetime.date or pd.NaT

    ...

    Examples:
    >>> dt_conv_mix('01-01-2021')
    datetime.date(2021, 1, 1)

    """
    if x == "":
        return pd.NaT

    return pd.to_datetime(x, format="mixed").date()

def fd_rooms_booked(x):
    """
    Convert the string to list

    This function takes the room booking column as a string and
    convert it into list format.

    Parameters:
    ----------

    x: str
        pass string

    Returns:
    -------
        Sting or np.nan
            The string value or np.nan if the input is empty

    Examples:
    >>> fd_rooms_booked('R10, R5')
    ['R10', 'R5']

    """
    if x == "":
        return np.nan
    else:
        x = str(x).replace(" ", "")
        return x.split(",")

def fd_upi_details(x):
    """
    Convert upi details colums sublist to uniform sublist.

    This is string manipulation function that removes the spaces, replaces 
    the semicolon with comma and split the string into list using " , " as
    a seperator.    

    Parameters:
    ----------
    x: list
        The input should be the list containing Sublist of upi details column

    Returns:
    -------
    list
        The 

    Examples:
    >>> fd_upi_details(['28022023, Bala;1470,06022023, Bala;1823,'])
    ['28022023', 'Bala', '1470', '06022023', 'Bala', '1823']

    """
    x = str(x).replace(" ", "")
    x = x.replace(";", ",").split(",")

    return [item for item in x if item != ""]

def bs_dep_amt(x):
    """
    Convert a string to a float and handle missing values.

        This function takes a string as input and returns
        a float. If the input is an empty string, it returns
        np.nan. Otherwise, it removes any commas from the
        input and converts it to a float.

    Parameters
    ----------
    x : str
        The input string to be converted.

    Returns
    -------
    float or np.nan
        The float value of the input string, or np.nan if the input is empty.

    Examples
    --------
    >>> bs_dep_amt("1,234.56")
    1234.56
    >>> bs_dep_amt("")
    nan
    """
    if x == "":
        return np.nan
    else:
        x = str(x)
        x = float(x.replace(",", ""))
        return x

def ota_pay_mix(x):
    """
    convert string to datetime.date by defining format "%d-%m-%Y"

    This function converts the string to the datatime by specifing the
    and Handling the missing valus.

    Parameters:
    ----------
    x : String
        The parameter should be the string format

    Returns:
    -------
    datetime.date or pd.nan
        The datetime.date of the input value or pd.NaT if it is empty

    Examples:
    --------
    >>> ota_pay_mix('01-01-2021')
    datetime.date(2021, 1, 1)
    """
    if x == "":
        return pd.NaT
    else:
        return pd.to_datetime(x, format="%d-%m-%Y").date()

def rm_quot(x):
    """ 
    Remove the single quote from the string
    ...
    This function takes the string as input and remove the single
    quote from the string.

    Parameters:
    ----------
    x: str
        pass string

    Returns:
    -------
    str
        The string value with removed single quotes,
        or np.nan if the input is empty

    Examples:
    --------
    >>> rm_quot(''Athul Sasidharan'')
    Athul Sasidharan
    """
    if x == "":
        return ""
    else:
        return x.replace("'", "")

def conv_price(x):
    """
    This function performs string manipulation

    This function takes the string as input and remove "INR"
    and convert it into float.

    Parameters:
    ----------
    x: str
        The string containing the "INR" and the price

    Returns:
    -------
    float
        The float value of the price

    Examples:
    --------
    >>> conv_price('INR 1,234.56')
    1234.56
    """
    if x == np.nan:
        return 0
    x = str(x)
    x = x.replace("INR", "")
    x = float(x)

    return x

def upi_date_time(x):
    """
    Split the list containing date and time

    ...
    
    This function takes the list as input loops through the list and
    checks the list for digits and append it to the new list. The new list
    is split into two seperate list containing date and amt. Its splitted
    based on:

    - i mod 2 = 0: date
    - i mod 2 = 1: amt

    To access the specific sublist use the index of the list.

    e.g.,
        1. upi_date_time()[0] gives the sublist of date.
        2. upi_date_time()[1] gives the sublist of amt.

    Parameters:
    ----------
    x: list
        list contains date, name and amt

    Returns:
    -------
    list
        list contains sublist of date and amt

    Examples:
    --------
    >>> upi_date_time(['28022023', 'Bala', '1470', '06022023', 'Bala', '1823'])
    [['28022023', '06022023'], ['1470', '1823']]
    """
    b, date, amt = [], [], []
    for i, val in enumerate(x):
        if val.isdigit():
            b.append(val)
    for i, val in enumerate(b):
        if i % 2 == 0:
            date.append(val)
        else:
            amt.append(val)

    return [date, amt]


# This dictonary holds corresponding 'converter functions' for different
# wrapper functions for loading data from various sources
Conv = {
    'front_desk': {
        "Name": str,
        "Phone": str,
        "Nights": int32_wrapper,
        "Adults": int32_wrapper,
        "Mode of Booking": str,
        "Rooms Booked": fd_rooms_booked,
        "Room Bill (Incl. GST)": float64_wrapper,
        "Extra Person Charges (Incl. GST)": float64_wrapper,
        "Advance Paid": float64_wrapper,
        "Advance Payment Method": str,
        "Check-in Payment Method": str,
        "Paid at Check-out": float64_wrapper,
        "Paid at Check-in": float64_wrapper,
        "Check-out Payment Method": str,
        "Extras Paid": float64_wrapper,
        "Extras Payment Method": str,
        "Total Amount Paid": float64_wrapper,
        "Status": str,
        "UPI Details": fd_upi_details,
    },
    'bank_statement': {
        "Deposit Amt (INR)": bs_dep_amt,
        "Transaction Date": dt_conv
        },
    'booking.com': {
        "Book Number": str,
        "Check-in": dt_conv,
        "Check-out": dt_conv,
        "Price": conv_price,
        "Commission Amount": conv_price,
    },
    'ingo_mmt_data': {
        "PNR": rm_quot,
        "Checkin Date": dt_conv_mix,
        "Checkout Date": dt_conv_mix,
        "Payments Date": ota_pay_mix,
        "Bank Ref No": rm_quot,
        "Commission Amount": conv_price
    },

}

## Wrapper functions for loading data from various sources

# front_desk data: Hospitality front desk
def fd_data(folder_path, FILE_NO):
    """
    Fetch Front Desk data transform the data using converter
    functions

    Parameters:
    ----------
    folder_path: str
        The path of the containing directory.

    Returns:
    --------
    pd.DataFrame:
        The dataframe containing manipulated data.

    Examples:
    ---------
    >>> fd_data('path_to_directory')
    DataFrame
    """

    fd_frame = create_frame(folder_path, FILE_NO, conv=Conv['front_desk'],
                            encode="utf-8")
    return fd_frame


# booking.com data: Booking.com/ Reservations
def bc_data(folder_path, FILE_NO):
    bc_frame = create_frame(folder_path, FILE_NO, conv=Conv['booking.com'])
    return bc_frame


# bank statement data: handles only ICICI
def bnk_state(folder_path, FILE_NO):
    bank_statements = create_frame(folder_path, FILE_NO, encode="utf-8",
                                   skphead=SKIPHEAD, skpfoot=SKIPFOOT,
                                   conv=Conv['bank_statement'])
    return bank_statements


# Paytm settlement data
def ptm_settle(folder_path, FILE_NO):
    paytm_settlement = create_frame(folder_path, FILE_NO)
    return paytm_settlement

# Paytm transactions data
def ptm_trans(folder_path, FILE_NO):
    paytm_transactions = create_frame(folder_path, FILE_NO)
    return paytm_transactions


# InGO-MMT data 
def ingo_mmt_data(folder_path, FILE_NO):
    ingo_mmt_data_ = create_frame(folder_path, FILE_NO,
                                  conv=Conv['ingo_mmt_data'])
    return ingo_mmt_data_