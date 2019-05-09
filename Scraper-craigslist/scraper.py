from urllib.request import Request, urlopen as uReq
from bs4 import BeautifulSoup as Soup
import math
import time
import pandas as pd
import numpy as np

"""Global variable to handle pagination. Its value will change once
the page is fetched"""
pagination_size = 1

"""Following information will be extracted"""
headers_list = ['TITLE', 'POSTED DATE', 'PRICE', 'BEDS',
                'BATHS', 'SQUARE FOOT', 'ADDRESS', 'FEATURES', 'LATITUDE', 'LONGITUDE', 'IMAGE LINK']


la_area_dict = {'sfv': 'SF valley', 'ant': 'Antelope valley', 'lac': 'Central LA',
                'lgb': 'Long beach', 'sgv': 'San gabriel valley', 'wst': 'Westside southbay'}


def fetch_page(url):
    time.sleep(3)
    """Opening up connection, grabbing the page! Returns HTML page"""
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    client = uReq(req)
    page_html = client.read()
    client.close()

    return page_html


def parse_html(page_html):
    """Html parsing"""
    return Soup(page_html, "html.parser")


def scraper(page_soup, rental_data):
    """Data extraction and formatting"""

    global pagination_size

    """Indicates that we have not yet updated pagination size.
    Please note that it will be updated only once."""
    if pagination_size == 1:
        pagination_size = int(page_soup.find('span', {'class': 'totalcount'}).text)
        pagination_size = math.ceil(pagination_size/120) - 1

    houses = page_soup.findAll('li', {'class': 'result-row'})

    for house in houses:
        internal_soup = ""
        try:
            address = None
            beds = None
            baths = None
            square_foot = None
            image_link = None
            price = house.find('span', {'class': 'result-price'}).text
            price = ''.join(e for e in price if e.isdigit())
            posted_date = house.find('time', {'class': 'result-date'}).text
            title = house.find('a', {'class': 'result-title hdrlnk'}).text
            href = house.find('a')['href']
            page_html = fetch_page(href)
            internal_soup = parse_html(page_html)

            location = internal_soup.findAll('div', {'id': 'map'})[0]
            latitude = location['data-latitude']
            longitude = location['data-longitude']

            features_list = internal_soup.findAll('p', {'class': 'attrgroup'})[-1].findAll('span')
            features = ""
            for i, feature in enumerate(features_list):
                features = features + str(i+1) + ". " + feature.text + " "

            bed_bath_sqfoot_details = internal_soup.select("span[class=shared-line-bubble]")

            for i, detail in enumerate(bed_bath_sqfoot_details):
                if i == 0:
                    beds = detail.findAll()[0].text
                    beds = ''.join(e for e in beds if e.isdigit())
                    baths = detail.findAll()[1].text
                    baths = ''.join(e for e in baths if e.isdigit())

                elif i == 1:
                    square_foot = detail.findAll()[0].text

            # bed_bath_details = internal_soup.findAll('span', {'class': 'shared-line-bubble'})
            # if len(bed_bath_details) > 0:
            #     bed_bath_details = bed_bath_details[0]
            #     beds = bed_bath_details.findAll()[0].text
            #     beds = ''.join(e for e in beds if e.isdigit())
            #     baths = bed_bath_details.findAll()[1].text
            #     baths = ''.join(e for e in baths if e.isdigit())
            #
            # else:
            #     beds = None
            #     baths = None
            #
            # square_foot = internal_soup.findAll('span', {'class': 'shared-line-bubble'})[1].findAll()
            # if len(square_foot) > 0:
            #     square_foot = square_foot[0].text
            # else:
            #     square_foot = None

            _address = internal_soup.findAll('div', {'class': 'mapaddress'})
            if len(_address) > 0:
                address = _address[0].text

            _image_link = internal_soup.findAll('img')
            if len(_image_link) > 0:
                image_link = _image_link[0]['src']

        except Exception as ex:
            template = "Some required information for was missing for a house " \
                       "like no of beds, baths, address, etc, So this house has been skipped."
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            continue

        rental_data.append((title, posted_date, price, beds, baths, square_foot, address,
                            features, latitude, longitude, image_link))

    return rental_data


def extract_data(url, rental_data):
    global pagination_size

    # Extracts data to rental_data
    page_html = fetch_page(url)
    rental_data = scraper(parse_html(page_html), rental_data)

    # For testing purpose
    print_first_few_rows(rental_data)

    # Handle pagination
    # for page in range(1, pagination_size):
    #     page_html = fetch_page(url + 's=' + str(120*page))
    #     rental_data = scraper(parse_html(page_html), rental_data)


    return rental_data


def get_panda_dataframe(rental_data):
    return pd.DataFrame(np.array(rental_data).reshape(len(rental_data), len(rental_data[0])),
                        columns=headers_list)


def export(file_name, pd_rental_data):
    pd_rental_data.to_csv(file_name, sep=',')


def print_first_few_rows(rental_data):
    print('Printing first few rows.\n')
    print(get_panda_dataframe(rental_data))


def perform_scraping():
    """Scrapping will be performed for 6 areas of los angeles (SF valley, Antelope valley,
    Central LA, Long beach, San gabriel valley, Westside southbay)"""
    global la_area_dict

    for key, value in la_area_dict.items():
        url = 'https://losangeles.craigslist.org/search/' + key + '/apa?'
        rental_data = []
        rental_data = extract_data(url, rental_data)
        pd_rental_data = get_panda_dataframe(rental_data)

        print(pd_rental_data)
        export(value + ".csv", pd_rental_data)


if __name__ == '__main__':
    perform_scraping()
