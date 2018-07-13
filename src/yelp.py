"""
Yelp Queries
------------
Uses Yelp Fusion Search API to query for businesses by a search term and
location, and the Business API to query additional information about the top
result from the search query.

Based on Yelp Fusion API code sample.
API documentation: https://www.yelp.com/developers/documentation/v3/get_started
"""
import functools
import pprint
import sys
from urllib.error import HTTPError
from urllib.parse import quote, urlencode

import requests

# OAuth credentials.
CLIENT_ID = ""
SECRET = ""

# API constants.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'

# Query constants.
SEARCH_LIMIT = 1


@functools.lru_cache(maxsize=100, typed=False)
def obtain_bearer_token (host, path):
    """Sends a POST request to the API for the bearer token.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.

    Returns:
        str: OAuth bearer token, obtained using client_id and client_secret.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    assert CLIENT_ID, "Please supply your client_id."
    assert SECRET, "Please supply your client_secret."
    data = urlencode({
        'client_id':CLIENT_ID,
        'client_secret':SECRET,
        'grant_type':GRANT_TYPE,
        })
    headers = {
        'content-type':'application/x-www-form-urlencoded',
        }
    response = requests.request('POST', url, data=data, headers=headers)
    return response.json()['access_token']


def request (host, path, bearer_token, url_params=None):
    """Given a bearer token, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        bearer_token (str): OAuth bearer token, obtained using client_id and
        client_secret.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {'Authorization':'Bearer %s'%bearer_token}
    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()


def search (token, term, location, sort_by='distance'):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
        sort_by (str): How to filter search results.

    Returns:
        dict: The JSON response from the request.
    """

    try:
        term = term.replace(' ', '+')
    except AttributeError:  # if not str
        term = str(term)

    try:
        location = location.replace(' ', '+')
    except AttributeError:  # if not str
        location = str(location)

    url_params = {
        'term':term,
        'location':location,
        'limit':SEARCH_LIMIT,
        'sort_by':sort_by
        }
    return request(API_HOST, SEARCH_PATH, token, url_params=url_params)


def get_business (business_id, token=None):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    if token is None:
        token = obtain_bearer_token(API_HOST, TOKEN_PATH)
    business_path = BUSINESS_PATH + business_id
    return request(API_HOST, business_path, token)


def get_business_match (term, location, sort_by='distance'):
    """Queries API for best match among businesses.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
        sort_by (str): How to filter search results.

    Returns:
        dict: The first JSON response from the request.
    """
    bearer_token = obtain_bearer_token(API_HOST, TOKEN_PATH)
    response = search(bearer_token, term, location, sort_by)
    businesses = response.get('businesses')

    if not businesses:
        return None

    return businesses[0]


def demo ():
    """Demo use of module for a single restaurant."""
    search_term = "KOITO JAPANESE RESTAURANT"
    location = '310 EAST 93 STREET, 10128'

    try:
        business_id = get_business_match(search_term, location)['id']
        print('Business id for best match of below params = {0}\n' \
              'Search Term: {1}\nLocation: {2}\n'.format(
              business_id, search_term, location))
        business_info = get_business(business_id)
        print('JSON dict for business with matching ID:\n')
        pprint.pprint(business_info)

    except HTTPError as error:
        sys.exit(
              'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                    error.code,
                    error.url,
                    error.read(),
                    )
              )


if __name__ == '__main__':
    demo()
