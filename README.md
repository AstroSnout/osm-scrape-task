# OSM Web Scraping task
Samples of scraped data can be found in the `Scraped Data` folder.

Requirements:  
* Python 3.9+

Packages used (3rd party):
* BeautifulSoup4
* configparser
* requests

Full package requirements can be found in the `requirements.txt` file.


### Feb 6th update:
* Implemented threading for faster scraping
* Fixed an issue where total page amount wasn't being calculated properly 

Note: It can go even faster, but it's been throttled to avoid sending too many requests at once.