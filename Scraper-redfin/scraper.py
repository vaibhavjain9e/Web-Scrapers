from urllib.request import Request, urlopen as uReq
from bs4 import BeautifulSoup as Soup
import json
import time
import pandas as pd
import numpy as np

"""Global variable to handle pagination. Its value will change once
the page is fetched"""
pagination_size = 1

"""Following information will be extracted"""
headers_list = ['SOLD DATE', 'ADDRESS', 'CITY', 'STATE', 'POSTAL CODE', 'PRICE', 'BEDS',
                'BATHS', 'PROPERTY TYPE', 'SQUARE FOOT']


def fetch_page(url):
    """Opening up connection, grabbing the page! Returns HTML page"""
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    client = uReq(req)
    page_html = client.read()
    client.close()

    return page_html


def parse_html(page_html):
    """Html parsing"""
    return Soup(page_html, "html.parser")


def scraper(page_soup, recently_sold_homes):
    """Data extraction and formatting"""

    global pagination_size

    """Indicates that we have not yet updated pagination size.
    Please note that it will be updated only once."""
    if pagination_size == 1:
        pagination_size = int(page_soup.findAll('a', {'class': 'clickable goToPage'})[-1].text)
    houses = page_soup.findAll('div', id=lambda x: x and x.startswith('MapHomeCard_'))

    for house in houses:
        try:
            json_data = json.loads(house.find('script').text)
            if isinstance(json_data, list):
                # if json data is a list, extract the first element as all others contain redundant info
                json_data = json_data[0]
            stats = house.findAll('div', {'class': 'stats'})
            sold_date = house.find('span', {'data-rf-test-id': 'home-sash'}).text.replace(',', ' ').split()[-3:]
            sold_date = ' '.join(str(x) for x in sold_date)
            property_type = json_data['@type']
            address = json_data['address']['streetAddress']
            city = json_data['address']['addressLocality']
            state = json_data['address']['addressRegion']
            postal_code = json_data['address']['postalCode']
            price = house.find('span', {'data-rf-test-name': 'homecard-price'}).text
            price = ''.join(e for e in price if e.isdigit())
            beds = ''.join(e for e in stats[0].text if e.isdigit())
            baths = ''.join(e for e in stats[1].text if e.isdigit())
            square_foot = ''.join(e for e in stats[2].text if e.isdigit())

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}. " \
                       "Some information might be missing. Please look at the extract_data function"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            continue

        recently_sold_homes.append((sold_date, address, city, state, postal_code, price, beds, baths,
                                    property_type, square_foot))

    return  recently_sold_homes


def extract_data(url, recently_sold_homes):
    global pagination_size

    # Extracts data to recently_sold_homes
    page_html = fetch_page(url)
    recently_sold_homes = scraper(parse_html(page_html), recently_sold_homes)

    # This call is made just for the purpose of this assignment(For docker). It will be removed in real-time
    print_first_few_rows(recently_sold_homes)

    # Handle pagination
    for page in range(2, pagination_size+1):
        page_html = fetch_page(url + 'page-' + str(page))
        recently_sold_homes = scraper(parse_html(page_html), recently_sold_homes)
        time.sleep(1)

    return recently_sold_homes


def get_panda_dataframe(recently_sold_homes):
    return pd.DataFrame(np.array(recently_sold_homes).reshape(len(recently_sold_homes), len(recently_sold_homes[0])),
                        columns=headers_list)


def export(pd_recently_sold_homes):
    pd_recently_sold_homes.to_csv("homes.csv", sep=',')


def print_first_few_rows(recently_sold_homes):
    print('Printing first few rows.\nNOTE: 1 second timer has been set between every call to redfin.com! '
          'Complete data will be printed as soon as scraping is completed.')
    print(get_panda_dataframe(recently_sold_homes))


def perform_scraping():
    """The url has been made static for this assignment. It will fetch the
    data of recently sold homes for Los Angeles. It can be made dynamic for
    multiple cities"""
    url = 'https://www.redfin.com/city/11203/CA/Los-Angeles/recently-sold/'
    recently_sold_homes = []
    recently_sold_homes = extract_data(url, recently_sold_homes)
    pd_recently_sold_homes = get_panda_dataframe(recently_sold_homes)

    print(pd_recently_sold_homes)
    export(pd_recently_sold_homes)


if __name__ == '__main__':
    perform_scraping()
