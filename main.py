import datetime as dt
import os
import requests
import csv
import configparser
from bs4 import BeautifulSoup


class Settings:
    cfgfile = 'settings.ini'
    _parser = configparser.RawConfigParser()
    _parser.read(cfgfile)

    @staticmethod
    def validate():
        outputdir = Settings._parser.get('config', 'outputdir')
        is_create_dir = Settings._parser.get('config', 'create_dir_if_no_exist')
        # If directory exist, we don't have to do anything more
        if os.path.isdir(outputdir):
            return
        else:
            # Check config if we are allowed to make the folder ourselves
            if is_create_dir == 'true':
                try:
                    # Try making the folder
                    os.mkdir(outputdir)
                    print(f'== Created directory {outputdir}')
                except OSError as e:
                    # Pops up if path is nonexistent or non-allowed characters are used - ?\<> etc
                    print(f'== {e.strerror}')
                    print(f'== Check your configuration file!')
                    input('Press any key to exit')
                    quit()
            elif is_create_dir == 'false':
                print("== Directory doesn't exist!")
            else:
                print('== Invalid configuration file! `is_create_dir` needs to be either "true" or "false"')

    @staticmethod
    def get_output_dir():
        return Settings._parser.get('config', 'outputdir')


class Requester:
    @staticmethod
    def get_soup(url):
        print(f'Requesting {url}')
        website = requests.get(url)
        return BeautifulSoup(website.content, 'html.parser')

    @staticmethod
    def post(url, params):
        curr_page = requests.post(url, data=params)
        return BeautifulSoup(curr_page.content, 'html.parser')

class MySoup:
    def __init__(self, soup):
        self.soup = soup

    def get_currencies(self):
        return [option['value'] for option in self.soup.find_all('option') if option['value'] != '0']

    def get_total_pages(self):
        # There's probably a prettier way of doing this
        total_pages = 0
        # Find all the script tags
        for scripts in self.soup.find_all('script'):
            # Break them up into lines
            for block in str(scripts).split('\n'):
                # Find the variable which says how many pages there are
                if 'var m_nPageSize = ' in block:
                    # Strip all the unnecessary characters, leaving only the number
                    total_pages = block.replace('var m_nPageSize = ', '').replace(';\r', '')
                    try:
                        total_pages = int(total_pages)
                    except AttributeError:
                        print('pages NaN')
                    break
        return total_pages

    def get_header(self):
        table = self.soup.find_all('table')[2]
        header_row = table.find_all('tr')[0]
        return header_row.find_all('td')

    def get_table_data(self):
        table = self.soup.find_all('table')[2]
        rows = table.find_all('tr')
        return [row.find_all('td') for row in rows[1:]]


def main():
    Settings.validate()
    url_to_scrape = 'https://srh.bankofchina.com/search/whpj/searchen.jsp'
    # Request the website and soupify the response
    soup = MySoup(
        Requester.get_soup(url_to_scrape)
    )
    # Get all currency options
    currencies = soup.get_currencies()
    # Getting dates for POST request params
    high_date = dt.datetime.today().strftime('%Y-%m-%d')
    low_date = (dt.datetime.today() - dt.timedelta(days=2)).strftime('%Y-%m-%d')
    # Going over the data for all listed currencies
    for currency in currencies:
        # Grabbing the first page
        print(f'== Getting data for {currency}')
        response_soup = Requester.post(
            url_to_scrape,
            {
                'erectDate': low_date,
                'nothing': high_date,
                'pjname': currency
            }
        )
        soup = MySoup(response_soup)
        # Grabbing total number of pages for requested currency
        total_pages = soup.get_total_pages()
        # Prepare the file
        filename = f'[{low_date}] [{high_date}] {currency}.csv'
        outputdir = Settings.get_output_dir()
        with open(f'{outputdir}/{filename}', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Look for the actual data we need on the page
            header_cells = soup.get_header()
            if len(header_cells) == 1:
                writer.writerow(['No records found!'])
                print(f'== No records found for {currency}!')
            else:
                writer.writerow([cell.text for cell in header_cells])
                # Do the same for every available page
                if total_pages >= 1:
                    for i in range(1, total_pages + 1):
                        print(f'== Scraping ({i}/{total_pages})')
                        response_soup = Requester.post(
                            url_to_scrape,
                            {
                                'erectDate': low_date,
                                'nothing': high_date,
                                'pjname': currency,
                                'page': i
                            }
                        )
                        data = MySoup(response_soup).get_table_data()
                        for row in data:
                            writer.writerow([cell.text for cell in row])
        print('===========')


main()
