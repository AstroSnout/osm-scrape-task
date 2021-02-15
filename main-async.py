import asyncio
import aiohttp
import datetime as dt
import os
import csv
import configparser
import math
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
                print('== Invalid configuration file! `create_dir_if_no_exist` needs to be either "true" or "false"')

    @staticmethod
    def get_output_dir():
        return Settings._parser.get('config', 'outputdir')


class Requester:
    _timeout = 60

    @staticmethod
    async def async_get_soup(url, run_until_ok=False):
        # Semaphore to regulate how many requests in parallel we're making
        async with semaphore:
            print('Requesting ->', url)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=Requester._timeout) as resp:
                    if resp.status == 200 or not run_until_ok:
                        return BeautifulSoup(await resp.text(), 'html.parser')
                    else:
                        print(f'Response status {resp.status}, trying again')
                        await asyncio.sleep(0.1)

    @staticmethod
    async def async_post(url, params, run_until_ok=False):
        # Semaphore to regulate how many requests in parallel we're making
        async with semaphore:
            print('Posting ->', params)
            async with aiohttp.ClientSession() as session:
                while True:
                    async with session.post(url, data=params, timeout=Requester._timeout) as resp:
                        if resp.status == 200 or not run_until_ok:
                            return BeautifulSoup(await resp.text(), 'html.parser')
                        else:
                            print(f'Response status {resp.status}, trying again')
                            await asyncio.sleep(0.1)


class MyQueue:
    def __init__(self):
        self._tasks_to_do = []
        self._running_tasks = []

    def _clear_queue(self):
        self._running_tasks = []

    def add(self, coro):
        self._tasks_to_do.append(coro)

    async def process(self):
        result = []
        task_res = await asyncio.gather(*self._running_tasks)
        result += task_res
        self._clear_queue()
        return result

    async def batch_process(self, concurrent=3):
        result = []
        while True:
            # If no more tasks queued
            if not self._tasks_to_do:
                # Process running tasks and break out of batch process loop
                if self._running_tasks:
                    task_res = await self.process()
                    result += task_res
                break
            # If we're not on concurrency limit, add more tasks to be done
            elif len(self._running_tasks) < concurrent:
                self._running_tasks.append(
                    self._tasks_to_do.pop(0)
                )
            else:
                task_res = await self.process()
                result += task_res
        return result


class MySoup:
    def __init__(self, soup):
        self.soup = soup

    def get_currencies(self):
        return [option['value'] for option in self.soup.find_all('option') if option['value'] != '0']

    def get_total_pages(self):
        # There's probably a prettier way of doing this
        total_pages = 1
        records_per_page = None
        total_records = None
        # Find all the script tags
        for scripts in self.soup.find_all('script'):
            # Break them up into lines
            for block in str(scripts).split('\n'):
                # Find the variable which says how many pages there are
                if 'var m_nRecordCount = ' in block:
                    # Strip all the unnecessary characters, leaving only the number
                    total_records = block.replace('var m_nRecordCount = ', '').replace(';\r', '')
                    try:
                        total_records = int(total_records)
                    except AttributeError:
                        print('pages NaN')
                if 'var m_nPageSize = ' in block:
                    # Strip all the unnecessary characters, leaving only the number
                    records_per_page = block.replace('var m_nPageSize = ', '').replace(';\r', '')
                    try:
                        records_per_page = int(records_per_page)
                    except AttributeError:
                        print('pages NaN')
                if records_per_page and total_records:
                    total_pages = math.ceil(total_records / records_per_page)
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


async def page_request_task(url, params):
    resp_soup = await Requester.async_post(
        url,
        params,
        run_until_ok=True
    )
    data = MySoup(resp_soup).get_table_data()
    index = params['page'] - 1
    return [index, data]


async def currency_batch_task(url_to_scrape, currency, low_date, high_date):
    queue = MyQueue()
    # Grabbing the first page
    print(f'== Getting data for {currency}')
    response_soup = await Requester.async_post(
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
    # Look for the actual data we need on the page
    header_cells = soup.get_header()
    if total_pages > 1:
        for pagenum in range(1, total_pages + 1):
            params = {
                'erectDate': low_date,
                'nothing': high_date,
                'pjname': currency,
                'page': pagenum
            }
            queue.add(
                page_request_task(url_to_scrape, params)
            )
        results = await queue.batch_process(concurrent=2)
        # Sorting data from coros in order in which they were queued
        all_data = [[] for x in range(total_pages)]
        for r in results:
            all_data[r[0]] = r[1]
        flat_all_data = [item for ls in all_data for item in ls]

    # Prepare the file
    filename = f'[{low_date}] [{high_date}] {currency}.csv'
    outputdir = Settings.get_output_dir()
    with open(f'{outputdir}/{filename}', 'w', newline='') as csvfile:
        # Writting the data
        writer = csv.writer(csvfile)
        # Header logic
        if len(header_cells) == 1:
            writer.writerow(['No records found!'])
            print(f'== No records found for {currency}!')
        else:
            writer.writerow([cell.text for cell in header_cells])
            # Writting data if available
            for row in flat_all_data:
                writer.writerow([cell.text for cell in row])


async def main():
    Settings.validate()
    url_to_scrape = 'https://srh.bankofchina.com/search/whpj/searchen.jsp'
    # Request the website and soupify the response
    soup = MySoup(
        await Requester.async_get_soup(url_to_scrape)
    )
    # Get all currency options
    currencies = soup.get_currencies()
    # Getting dates for POST request params
    high_date = dt.datetime.today().strftime('%Y-%m-%d')
    low_date = (dt.datetime.today() - dt.timedelta(days=2)).strftime('%Y-%m-%d')
    # Going over the data for all listed currencies
    queue = MyQueue()
    for currency in currencies:
        queue.add(
            currency_batch_task(url_to_scrape, currency, low_date, high_date)
        )
    await queue.batch_process(concurrent=50)

# Globally available semaphore to regulate concurrent tasks performed
semaphore = asyncio.Semaphore(10)
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
