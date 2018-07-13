"""
Linear Modeling
---------------

Playground to test different linear models.
"""

import getpass
import numpy as np
import pandas as pd

from timer import ProgramTimer

pd.options.mode.chained_assignment = None
log = ProgramTimer(ud_start=True, ud_end=False)
log._start_ud_pre_txt = ''

#####################################################################
# Paths to key datasets.

PATH_YELP_DATA = '../data/yelp_data.csv'
PATH_INSPECTIONS = '../data/nyc_restaurant_inspection_data.csv'

#####################################################################
# Methods to load key datasets.

def _clean_street_address (address):
    """Removes redundant spaces in STREET column."""
    try:
        return ' '.join(address.split())
    except:
        return address


def _create_full_address (row):
    """Create representation of address in same format as the address field
    from Yelp's API.
    """
    if row['BUILDING'] == 'NKA':
        return row['STREET']
    return '{0} {1}'.format(row['BUILDING'], row['STREET'])


def load_inspections (url=PATH_INSPECTIONS):
    """Creates DF from NYC inspections data and handles preliminary
    data-cleaning. Sets CAMIS as index.

    Raises:
        RuntimeError if number of rows after cleaning data are fewer than 20K.
    """
    df = pd.read_csv(url)
    # Remove records not containing a zip code and ensure zip code is an int.
    df = df[df['ZIPCODE'].notnull()]
    df['ZIPCODE'] = df['ZIPCODE'].astype(np.int64)
    # Format street address.
    df['STREET'] = df['STREET'].apply(_clean_street_address)
    # Create full address used for Yelp API.
    df['FULL_ADDRESS'] = df.apply(_create_full_address, axis=1)
    df['YELP_ID'] = np.NaN  # will be added later.
    df.set_index('CAMIS', inplace=True, drop=False)
    if len(df.index) < 20000:
        raise RuntimeError('Inspections DF contains < 20,000 rows.')
    return df


def load_yelp (url=PATH_YELP_DATA):
    """Sets CAMIS as index."""
    df = pd.read_csv(url, encoding='ISO-8859-1')
    df.set_index('CAMIS', inplace=True, drop=False)
    return df


#####################################################################
# Reading / writing JSON and Excel.

nyc_df = load_inspections()
yelp_df = load_yelp()
camis_with_yelp = yelp_df['CAMIS'].unique().tolist()
print('Number of CAMIS in yelp_df: {}\n'.format(len(camis_with_yelp)))

nyc_df = nyc_df[nyc_df['CAMIS'].isin(camis_with_yelp)]
nyc_df.drop_duplicates('CAMIS', inplace=True)
print('Rows in NYC DF after removing duplicate CAMIS: {}\n'.format(len(
      nyc_df.index)))
