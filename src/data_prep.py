"""
Data Preparation
----------------

Various scripts that were used to build the Yelp dataset.

Assumptions that were made:
*   Top business result returned from Yelp API

"""


import getpass
import json

import numpy as np
import pandas as pd

from timer import ProgramTimer
import yelp
pd.options.mode.chained_assignment = None
log = ProgramTimer(ud_start=True, ud_end=False)
log._start_ud_pre_txt = ''

#####################################################################
# Paths to key datasets.

PATH_YELP_DATA = '../data/yelp_data.csv'
PATH_INSPECTIONS = '../data/nyc_restaurant_inspection_data.csv'

#####################################################################
# Reading / writing JSON and Excel.

def load_yelp_data (url):
    """Loads Yelp data saved on local computer at given url."""
    data = []
    decoder = json.JSONDecoder()
    with open(url, 'r', encoding='utf-8') as src:
        for line in src:
            obj, idx = decoder.raw_decode(line)
            data.append(obj)
    return data


def append_json_to_file (data, url):
    """Appends JSON to existing .txt file."""
    with open(url, 'a+', encoding='utf-8') as f:
        for row in data:
            json.dump(row, f, ensure_ascii=False)
            f.write('\n')


def write_json_to_file (data, url):
    """Saves JSON to .txt file.

    Args:
        data (List[dict]): Data to write to file.
        url (str): Path to save file.
    """
    with open(url, 'w', encoding='utf-8') as f:
        for row in data:
            json.dump(row, f, ensure_ascii=False)
            f.write('\n')


#####################################################################
# Functions used to load / add new fields to NYC inspection results DataFrame.

def clean_street_address (address):
    """Remove redundant spaces in STREET column."""
    try:
        return ' '.join(address.split())
    except:
        return address


def create_full_address_old (row):
    """Create location search term used for Yelp API by combining fields.
    Was used to build address used as the location search term for Yelp API.
    """
    if row['BUILDING'] == 'NKA':
        return '{0}, {1}'.format(row['STREET'], row['ZIPCODE'])
    return '{0} {1}, {2}'.format(row['BUILDING'], row['STREET'], row['ZIPCODE'])


def create_full_address (row):
    """Create representation of address in same format as the address field
    from Yelp's API.
    """
    if row['BUILDING'] == 'NKA':
        return row['STREET']
    return '{0} {1}'.format(row['BUILDING'], row['STREET'])


def load_inspection_data (url=PATH_INSPECTIONS):
    """Creates DF from NYC inspections data and handles preliminary
    data-cleaning.
    """
    df = pd.read_csv(url)
    # Remove records not containing a zip code and ensure zip code is an int.
    df = df[df['ZIPCODE'].notnull()]
    df['ZIPCODE'] = df['ZIPCODE'].astype(np.int64)
    # Format street address.
    df['STREET'] = df['STREET'].apply(clean_street_address)
    # Create full address used for Yelp API.
    df['FULL_ADDRESS'] = df.apply(create_full_address, axis=1)
    df['YELP_ID'] = np.NaN  # will be added later.
    return df


def load_inspections_yelp_bridge (url='inspection_yelp_bridge.csv'):
    df = pd.read_csv(url, names=['CAMIS', 'YELP_ID'], encoding='ISO-8859-1')
    df.set_index('CAMIS', inplace=True, drop=False)
    return df


#####################################################################
# Create two datasets that we'll save:
#   (1) Yelp JSON containing information for businesses in NYC inspections data.
#   (2) DataFrame containing CAMIS (from NYC inspections data) and id from
#       Yelp business queries in order to pair the two datasets.

def update_yelp_data (row_count, inspections_df=None):
    """Saves row_count number of Yelp business JSON entries to existing
    datasets.
    """
    if not inspections_df:
        inspections_df = load_inspection_data(PATH_INSPECTIONS)

    new_biz_json = []

    def find_business_id (row):
        """Queries Yelp API for id of best-matching JSON business response.

        ALSO adds the business JSON to module-level list for eventual saving.
        """
        nonlocal new_biz_json
        search_term = row['DBA']  # business name
        location = row['FULL_ADDRESS']
        biz_json = yelp.get_business_match(search_term, location)

        if biz_json is None:
            return np.NaN

        # Ensure the business id will be able to be encoded in final csv.
        # If not, we can't use it.
        try:
            biz_id = biz_json['id'].encode('ISO-8859-1')
        except:
            print('Failed encoding business id for CAMIS: {}'.format(
                  row['CAMIS']))
            return np.NaN

        # Append this JSON to full list.
        new_biz_json.append(biz_json)

        return biz_json['id']

    log.start('Loading NYC Inspections data with no corresponding Yelp id')

    # Load NYC inspections data and Yelp-inspections bridge.
    # Filter inspections data to only those (unique) records for which a Yelp ID
    # has not been recovered
    bridge_df = load_inspections_yelp_bridge('inspection_yelp_bridge.csv')
    camis_done = bridge_df['CAMIS'].unique().tolist()
    inspections_df = inspections_df[~inspections_df['CAMIS'].isin(camis_done)]
    # Preserve only rows with unique CAMIS since right now we only care about
    # finding business details per CAMIS.
    inspections_df.drop_duplicates('CAMIS', inplace=True)

    # Work on only a subset of NYC inspections (to avoid breaching Yelp API
    # daily limits).
    inspections_subset = inspections_df.iloc[:row_count]
    log.end()

    log.start('Querying Yelp API for business id per CAMIS')
    # find_business_id() function adds Yelp business JSON entry to business_json
    # list during the process of obtaining the id for the business.
    inspections_subset['YELP_ID'] = inspections_subset.apply(find_business_id,
                                                             axis=1)
    log.end()

    # Create DataFrame of just CAMIS and Yelp ID to save.
    log.start('Saving updated CAMIS/Yelp ID bridge')
    additional_bridge_data = inspections_subset[['CAMIS', 'YELP_ID']]
    additional_bridge_data.set_index('CAMIS', inplace=True, drop=False)
    # Append these new rows to existing bridge DF.
    bridge_df = bridge_df.append(additional_bridge_data, ignore_index=True)
    try:
        bridge_df.to_csv('inspection_yelp_bridge.csv', encoding='ISO-8859-1')
    except Exception as e:
        print('DF that caused error:')
        print(additional_bridge_data)
        raise
    finally:
        log.end()

    log.start('Appending new Yelp business JSON to file')
    append_json_to_file(new_biz_json, 'yelp_data.txt')
    log.end()

    log.print_summary('Finished')


def query_additional_yelp_business_json (blocks, queries_per_block):
    """Query Yelp API for business data in chunks in order to save
    intermittently.
    """
    inspections_df = load_inspection_data(PATH_INSPECTIONS)
    for block in range(0, blocks):
        print('Executing block {}\n'.format(block + 1))
        update_yelp_data(queries_per_block, inspections_df)


#####################################################################
# Creating a DataFrame out of the Yelp business JSON data.

def _get_categories (row):
    """Returns list of aliases from categories entry."""
    result = []
    try:
        for category in row['categories']:
            result.append(category['alias'])
    except:
        log.log_err('_get_categories', row['id'])
        return np.NaN
    if not result:
        return np.NaN
    return result


def _get_transactions (row):
    transactions = row['transactions']
    if not transactions:
        return np.NaN
    return transactions


def _get_item (row, fld):
    """Encapsulate retrieving item from JSON so that an NaN can be returned
    when that field is not present.
    """
    try:
        return row[fld]
    except KeyError:
        log.log_err('_get_item', row['id'], fld)
        return np.NaN


def _get_price (row):
    try:
        return row['price'].count('$')
    except KeyError:
        log.log_err('_get_price', row['id'], 'price')


def _get_coord (row, coord):
    try:
        return float(row['coordinates'][coord])
    except:
        log.log_err('_get_coord', row['id'], coord)
        return np.NaN


def _create_df_entry_from_json (row):
    """Creates row to be appended onto DataFrame."""
    new_row = {'id':row['id'],
               'name':row['name'],
               'url':_get_item(row, 'url'),
               'phone':_get_item(row, 'phone'),
               'latitude':_get_coord(row, 'latitude'),
               'longitude':_get_coord(row, 'longitude'),
               'review_count':int(row['review_count']),
               'price':_get_price(row),
               'rating':float(row['rating']),
               'transactions':_get_transactions(row),
               'categories':_get_categories(row),
               'address':row['location']['address1'],
               'city':row['location']['city'],
               'state':row['location']['state'],
               'zip_code':row['location']['zip_code']
               }
    return new_row


def create_yelp_data_df ():
    """Saves DataFrame made out of Yelp business JSON and error log."""
    df = pd.DataFrame(
          columns=['id', 'name', 'url', 'phone', 'latitude', 'longitude',
                   'review_count', 'price', 'rating', 'transactions',
                   'categories', 'address', 'city', 'state', 'zip_code'])

    json_data = load_yelp_data('yelp_data.txt')

    for row in json_data:
        new_row = _create_df_entry_from_json(row)
        try:
            name_decoded = new_row['name'].encode('ISO-8859-1')
            id_decoded = new_row['id'].encode('ISO-8859-1')
        except:
            log.log_err('Creating row', 'na', 'name or id not encodable.')
            continue
        df = df.append(new_row, ignore_index=True)

    df.set_index('id', inplace=True, drop=True)
    df.to_csv('yelp_data.csv', encoding='ISO-8859-1')
    log.errors.to_csv('error_log.csv')

def load_yelp_df (url='yelp_data.csv'):
    df = pd.read_csv(url, encoding='ISO-8859-1')
    df['CAMIS'] = df.apply(get_camis, axis=1)
    df.set_index('CAMIS', inplace=True, drop=False)
    return df

#####################################################################
# Double-checking match between NYC Inspections and Yelp address to ensure
# the business match is correct.

# Filter NYC Inspections to rows with unique CAMIS for which there is a
# YELP_ID in bridge table.
nyc_df = load_inspection_data()
bridge_df = load_inspections_yelp_bridge()
camis_in_bridge = bridge_df['CAMIS'].unique().tolist()
print('unique CAMIS count: {}'.format(len(camis_in_bridge)))

nyc_df = nyc_df[nyc_df['CAMIS'].isin(camis_in_bridge)]
print('Rows in NYC DF after limiting to Yelp universe: {}\n'.format(len(
      nyc_df.index)))

nyc_df.drop_duplicates('CAMIS', inplace=True)
nyc_df.set_index('CAMIS', inplace=True, drop=False)

print('Rows in NYC DF after removing dupes: {}\n'.format(len(
      nyc_df.index)))


# Load Yelp data, append CAMIS from Inspections data, and set index to CAMIS.
def get_camis (yelp_row):
    row_match = bridge_df[bridge_df['YELP_ID'] == yelp_row['id']].iloc[0]
    return row_match['CAMIS']


yelp_df = load_yelp_df()
yelp_df.to_csv('yelp_data v2.csv', encoding='ISO-8859-1')

print('Rows in Yelp DF: {}'.format(len(yelp_df.index)))
print('Rows in Inspections DF: {}\n'.format(len(nyc_df.index)))

print('Columns in Yelp DF: {}\n'.format(yelp_df.columns))
print('Columns in Inspections DF: {}\n'.format(nyc_df.columns))

merged_df = pd.merge(nyc_df, yelp_df, on='CAMIS')
print('Rows in joint DF: {}'.format(len(merged_df.index)))