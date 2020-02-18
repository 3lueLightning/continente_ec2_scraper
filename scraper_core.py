import time
import boto3
import pickle
import logging
import hashlib
import pandas as pd
from pathlib import Path
from datetime import date, datetime
from random import randint, uniform
from bs4 import BeautifulSoup as soup
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException


PRODUCTS_HTML_FN = 'products_html.pkl'
CATEGORIES_FN = 'product_categories.csv'
BASE_DIR = "/home/ubuntu/scraping/"
BUCKET_NAME = 'continente-scraping'
BUCKET_KEY_BASE = 'product-containers/catalogue-'
CATEGORIES_TABLE_KEY = 'categories_table.csv'
LOGGING_LEVEL = logging.DEBUG
LOGGING_FN = 'parser.log'
SCRAPING_STATUS_FN = 'scraping_status.csv'
# if a file with category and category_lvl1 columns is selected then only the
# page matching the subcategories selected will be scraped
SELECTED_CATEGORIES_FN_PATH = None
#if the value here is NONE all page are taken otherwise only the selected number of pages is sampled
N_SAMPLE_CATEGORIES = 2#None
N_SAMPLE_SUBCATEGORIES = 2#None
N_SAMPLE_PAGES = None

popular_useragents = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"]


def click_category(driver, category_id, category):
  """
  clicks in a category, so it can reveal its subcategories in various ways
  """
  button = driver.find_element_by_id(category_id)
  try:
    try:
      button.click()
      logger.debug('click - success')
    except ElementClickInterceptedException:
      link = button.find_element_by_link_text(category)
      driver.execute_script("arguments[0].click();", link)
      logger.debug('click - successful (backup)')
  except:
    logger.debug('click - failed')
    return False
  return True


def categories_lvl1_url(category):
  base_url = 'https://www.continente.pt/stores/continente/pt-pt/public/Pages/subcategory.aspx?cat='
  return base_url + category


def parse_name(container):
  """
  parses a product name in a container
  """
  description_html = container.findChild("div",{"class":"containerDescription"})
  try:
    return description_html.div.a["title"]
  except:
    return None


def scrape_containers(page_html):
  """
  takes a page's raw HTML and extracts the product containers from it
  """
  page_soup = soup( page_html, 'html.parser') # parse webpage
  # select product divs
  containers = page_soup.findAll("div",{"class":"productBoxTop"})
  try:
    logger.debug([parse_name(container) for container in containers][:3])
  except:
    logger.warning('unable to parse container names')
  return containers


def get_last_page_simple(driver, wait=7, n_attempts=3):
  xpath = "//div[@class='pagingInnerArea _asyncPaginationWrapper']//li[last()]"
  for i in range(1, n_attempts + 1):
    try:
      logger.debug(f'function get_last_page - waiting {i * wait}s - attempt number {i}')
      time.sleep(i * wait)
      last_page = driver.find_element_by_xpath(xpath).text
    except (NoSuchElementException, StaleElementReferenceException):
      last_page = None
    else:
      last_page = int(last_page)
      break
  return last_page


def get_last_page(driver, wait=7, n_attempts=3, verification=True):
  """
  It looks for the last page of a category, if it doesn't find the element
  then it waits some more for it to load, if that still doesn't show up then
  it means that there isn't a last page meaning there is only one page\n\n

  driver: a selenium webdriver\n
  wait: time to wait for the first response\n
  n_attempts: number of attempts to find the last page\n
  verification: use some logic to check that the last page detected makes sense\n
  """
  last_page = get_last_page_simple(driver, wait=wait, n_attempts=n_attempts)
  n_containers = len( scrape_containers(driver.page_source))
  if last_page and verification and n_containers <= 20 and last_page > 1:
    last_page = get_last_page_simple(driver, 3 * (wait + 1), n_attempts=1)
  return last_page


def scrape_subcategory(driver, url):
  """
  Takes the url of the initial page of a page from any category of products,
  identifies how many pages there are and then scrapes through all of them\n
  url: url until the #/? characters (not included)
  """
  products = []
  url += '#/?pl=80'
  driver.get(url)
  logger.debug(f'first page url: {driver.current_url}')
  last_page = get_last_page(driver, wait=uniform(4,6))
  if not last_page:
    logger.error('last page not found - unable to extract subcategory')
    return []
  logger.info(f'the subcategory contains {last_page} pages')
  # sample pages during code debugging
  if N_SAMPLE_PAGES and last_page > N_SAMPLE_PAGES:
    last_page = N_SAMPLE_PAGES
    logger.warning(f'sampled only the first: {N_SAMPLE_PAGES}')
  hash_list = []
  for page in range(1, last_page + 1):
    driver.get( url + '&page=' + str(page))
    logger.debug(f'page {page} url: {driver.current_url}')
    time.sleep(uniform(3,5))
    #when the connection is slow ajax doesn't refresh the page quickly enough and the first page of subcategory is
    #displayed instead of the desired page. To make sure this doesn't happen we compare the hash of the first page to
    #the hash of the current one. If they are identical the code waits longer and loads the page one more time
    page_hash = hashlib.md5(str(driver.page_source).encode('utf-8')).digest()
    if page_hash in hash_list:
      logger.info('refresh problem same page loaded, waiting to refresh')
      driver.get( url + '&page=' + str(page))
      time.sleep(10)
      page_hash = hashlib.md5(str(driver.page_source).encode('utf-8')).digest()
      if page_hash in hash_list:
        logger.warning('tried refreshing the page but failed, continuing')
        continue
    products.extend( scrape_containers(driver.page_source))
    hash_list.append( page_hash)
  return products


def scraping_status_updater(status_df, category, category_lvl1, success, n_products):
    new_status = {'category': category, 'category_lvl1': category_lvl1,
                  'success': success, 'n_products': n_products,
                  'creation_date': datetime.now().strftime("%Y-%m-%d %H:%M")}
    status_df = status_df.append(new_status, ignore_index=True)
    status_df[-1:].to_csv(BASE_DIR + SCRAPING_STATUS_FN, mode='a', header=False, index=False)
    return status_df


def category_to_files_scraper(driver, categories_menu, categories_table, selected_categories):
  Path(BASE_DIR).mkdir(parents=True, exist_ok=True)
  scraping_status_df = pd.DataFrame({'category': [], 'category_lvl1': [],
                                     'success': [], 'n_products': [],
                                     'creation_date': []})
  scraping_status_df = scraping_status_df.astype({'success': int, 'n_products': int})
  scraping_status_df.to_csv(BASE_DIR + SCRAPING_STATUS_FN, index=False)
  products_by_subcat = {}
  for category, category_id in categories_menu[1:]:
    time.sleep(uniform(8,15))
    logger.info('\n'+ 10*'=' + category + 10*'=')
    successful_click = click_category(driver, category_id, category)
    if successful_click:
      time.sleep(uniform(3,5))
      subcat_nav = driver.find_elements_by_xpath(f"//li[@id='{category_id}']//li")
      subcategory_ids = [(elem.text.strip(), elem.get_property('id')) for elem in subcat_nav]
      if N_SAMPLE_SUBCATEGORIES:
        subcategory_ids = subcategory_ids[:N_SAMPLE_SUBCATEGORIES + 1]
      # iterate from the 2nd element to avoid getting the 1st category: '(todos)'
      for category_lvl1, category_lvl1_id in subcategory_ids[1:]:
        category_lvl1 = category_lvl1.strip("'| |\"")
        # need to explicitly test 'is not None' bellow
        if selected_categories is not None and \
                selected_categories.query(f"category == '{category}' & category_lvl1 == '{category_lvl1}'").empty:
          logger.info(f'category_lvl1 "{category_lvl1}" NOT selected for scraping')
          continue
        logger.info(f'category_lvl1 "{category_lvl1}" selected for scraping')
        time.sleep(uniform(2,4))
        try:
          url = categories_lvl1_url( category_lvl1_id)
          driver.get(url)
        except InvalidArgumentException:
          logger.debug('failed to get subcategory URL')
          scraping_status_df = scraping_status_updater(scraping_status_df, category, category_lvl1, False, 0)
          continue
        logger.info( f'success for category_lvl1: {category_lvl1}')
        category_match = categories_table.query(f"category == '{category}' & category_lvl1 == '{category_lvl1}'")
        if not category_match.empty:
          internal_category_id = category_match.index[0]
          logger.debug('scraped category matched to existing category')
        else:
          new_subcategory = pd.DataFrame({'category': [category],
            'category_lvl1': [category_lvl1], 'creation_date': [date.today()]})
          categories_table = categories_table.append( new_subcategory)
          new_subcategory.to_csv(BASE_DIR + CATEGORIES_FN, mode='a', header=False, index=False)
          internal_category_id = categories_table.index[-1]
          logger.debug('new category and/or subcategory created')
        try:
          subcat_products = scrape_subcategory(driver, url)
        except WebDriverException:
          logger.debug('subcategory scraping failed')
          scraping_status_df = scraping_status_updater(scraping_status_df, category, category_lvl1, False, 0)
          continue
        logger.debug('updating product dictionaries')
        products_by_subcat[internal_category_id] = subcat_products
        # converting the beautiful soup html into string to be able to pickle it and storing it in dict
        subcat_products_html = {internal_category_id: [str(prod) for prod in subcat_products]}
        with open(BASE_DIR + PRODUCTS_HTML_FN, 'ab') as storage_file:
          pickle.dump( subcat_products_html, storage_file)
          scraping_status_df = scraping_status_updater(scraping_status_df, category, category_lvl1, True, len(subcat_products))
    else:
      logger.debug('click failed')
      scraping_status_df = scraping_status_updater(scraping_status_df, category, 'ALL', False, 0)
  logger.info('scraping status')
  logger.info(scraping_status_df)
  return products_by_subcat


def find_categories(driver):
  start_url = "https://www.continente.pt/stores/continente/pt-pt/public/Pages/category.aspx?cat=Mercearia#/"
  driver.get(start_url)
  time.sleep(4)
  categories_menu_nav = driver.find_elements_by_xpath("//ul[@id='categoryMenu']//li")
  categories_menu = [(li.text.strip(), li.get_property('id')) for li in categories_menu_nav]
  if N_SAMPLE_CATEGORIES:
    categories_menu = categories_menu[:N_SAMPLE_CATEGORIES + 1]
    logger.warning(f'only the first {N_SAMPLE_CATEGORIES} were selected')
  logger.info(categories_menu)
  if not categories_menu:
    logger.critical('unable to extract product categories')
  return categories_menu


def download_or_create_categories_table(s3):
  Path(BASE_DIR).mkdir(parents=True, exist_ok=True)
  try:
    s3.Bucket(BUCKET_NAME).Object(CATEGORIES_FN).download_file(BASE_DIR + CATEGORIES_FN)
    logger.debug('categories_table downloaded from S3')
  except ClientError:
    categories_table = pd.DataFrame({'category': [], 'category_lvl1': [], 'creation_date': []})
    categories_table.to_csv(BASE_DIR + CATEGORIES_FN, index=False)
    logger.debug('categories_table created')
  else:
    categories_table = pd.read_csv(BASE_DIR + CATEGORIES_FN)
  return categories_table



logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(LOGGING_FN)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler()) # TODO: remove output to console
logger.info('scraping starting')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
user_agent = popular_useragents[randint(0, len(popular_useragents) - 1)]
chrome_options.add_argument("user-agent=" + user_agent)


wd = webdriver.Chrome('chromedriver', options=chrome_options)
logger.debug(wd.execute_script("return navigator.userAgent")) # printing out the user agent
categories_menu = find_categories( wd)
s3 = boto3.resource('s3')
categories_table = download_or_create_categories_table(s3)
if SELECTED_CATEGORIES_FN_PATH:
  selected_categories_df = pd.read_csv(SELECTED_CATEGORIES_FN_PATH)
  logger.info('only scraping selected categories')
else:
  selected_categories_df = None

category_to_files_scraper(wd, categories_menu, categories_table, selected_categories_df)
wd.quit()


s3.Bucket(BUCKET_NAME).Object(BUCKET_KEY_BASE + str(date.today())).upload_file(BASE_DIR + PRODUCTS_HTML_FN)
s3.Bucket(BUCKET_NAME).Object(CATEGORIES_FN).upload_file(BASE_DIR + CATEGORIES_FN)
s3.Bucket(BUCKET_NAME).Object(SCRAPING_STATUS_FN).upload_file(BASE_DIR + SCRAPING_STATUS_FN)