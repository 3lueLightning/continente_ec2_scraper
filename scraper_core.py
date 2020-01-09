import os
import time
import boto3
import pickle
import hashlib
import pandas as pd
from pathlib import Path
from datetime import date
from collections import defaultdict
from random import randint, uniform
from bs4 import BeautifulSoup as soup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import ElementClickInterceptedException



products_html_fn = 'products_html.pkl'
categories_fn = 'product_categories.csv'
base_dir = "/home/ubuntu/scraping/"
bucket_name = 'continente-scraping'
bucket_key_base = 'product-containers/catalogue-'

popular_useragents = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"]

def click_category(category_id):
  """
  clicks in a category, so it can reveal its subcategories in various ways
  """
  button = wd.find_element_by_id(category_id)
  try:
    try:
      button.click()
    except ElementClickInterceptedException:
      link = button.find_element_by_link_text(category)
      wd.execute_script("arguments[0].click();", link)
  except:
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
  #REMOVE just for debugging
  try:
    print([parse_name(container) for container in containers][:3])
  except:
    print('unable to parse container names')
  #REMOVE end
  return containers


def get_last_page_simple(driver, wait_0=0, wait_1=10):
  time.sleep(wait_0)
  xpath = "//div[@class='pagingInnerArea _asyncPaginationWrapper']//li[last()]"
  try:
    last_page = driver.find_element_by_xpath(xpath).text
  except NoSuchElementException:
    time.sleep(wait_1)
    try:
      last_page = driver.find_element_by_xpath(xpath).text
    except NoSuchElementException:
      return 1
  return int(last_page)


def get_last_page(driver, wait_0=5, wait_1=10, verification=True):
  """
  It looks for the last page of a category, if it doesn't find the element
  then it waits some more for it to load, if that still doesn't show up then
  it means that there isn't a last page meaning there is only one page\n\n

  driver: a selenium webdriver\n
  wait_0: time to wait for the first response\n
  wait_1: time to wait for the second response (if first failed)\n
  """
  last_page = get_last_page_simple(driver, wait_0=wait_0, wait_1=wait_1)
  print(last_page)
  n_containers = len( scrape_containers(driver.page_source))
  if verification and n_containers <= 20 and last_page > 1:
    last_page = get_last_page_simple(driver, 10)
  return last_page


def scrape_subcategory(url):
  """
  Takes the url of the initial page of a page from any category of products,
  identifies how many pages there are and then scrapes through all of them\n
  url: url until the #/? characters (not included)
  """
  products = []
  url += '#/?pl=80'
  wd.get(url)
  last_page = get_last_page(wd, wait_0=uniform(4,6))
  #REMOVE: just for testing
  print('REMOVE for testing constraint on pages')
  print(wd.current_url)
  print('real last_page: ' + str(last_page))
  #last_page = min(last_page, 3) # remove
  #print('truncated last_page: ' + str(last_page))
  #REMOVE end
  hash_list = []
  for page in range(1, last_page + 1):
    wd.get( url + '&page=' + str(page))
    print(wd.current_url)
    time.sleep(uniform(3,5))
    page_hash = hashlib.md5(str(wd.page_source).encode('utf-8')).digest()
    if page_hash in hash_list:
      print('same hash, wait')
      wd.get( url + '&page=' + str(page))
      time.sleep(10)
      page_hash = hashlib.md5(str(wd.page_source).encode('utf-8')).digest()
      if page_hash in hash_list:
        print('same has, wait failed continuing')
        continue
    products.extend( scrape_containers(wd.page_source))
    hash_list.append( page_hash)
  return products


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
user_agent = popular_useragents[randint(0,len(popular_useragents) - 1)]
chrome_options.add_argument("user-agent=" + user_agent)


wd = webdriver.Chrome('chromedriver', options=chrome_options)
print(wd.execute_script("return navigator.userAgent")) # printing out the user agent
start_url = "https://www.continente.pt/stores/continente/pt-pt/public/Pages/category.aspx?cat=Mercearia#/"
wd.get(start_url)
time.sleep(4)
categories_menu_nav = wd.find_elements_by_xpath("//ul[@id='categoryMenu']//li")
categories_menu = [(li.text.strip(), li.get_property('id')) for li in categories_menu_nav]
print(categories_menu)


Path(base_dir).mkdir(parents=True, exist_ok=True)
if os.path.exists(base_dir + categories_fn):
  categories_table = pd.read_csv( base_dir + categories_fn)
else:
  categories_table = pd.DataFrame({'category': [], 'category_lvl1': []})
  categories_table.to_csv( base_dir + categories_fn, index=False)
print(categories_table)


failed_categories = []
successful_categories_lvl1 = defaultdict(list)
failed_categories_lvl1 = defaultdict(list)
for category, category_id in categories_menu[1:]:
  time.sleep(uniform(8,15))
  print('\n'+ 10*'=' + category + 10*'=')
  successful_click = click_category(category_id)
  if successful_click:
    time.sleep(uniform(3,5))
    subcat_nav = wd.find_elements_by_xpath(f"//li[@id='{category_id}']//li")
    subcategory_ids = [(elem.text.strip(), elem.get_property('id')) for elem in subcat_nav]
    # iterate from the 2nd element to avoid getting the 1st category: '(todos)'
    for category_lvl1, category_lvl1_id in subcategory_ids[1:]:
      time.sleep(uniform(2,4))
      try:
        url = categories_lvl1_url( category_lvl1_id)
        wd.get(url)
      except InvalidArgumentException:
        print('FAILED: ' + category_lvl1)
        failed_categories_lvl1[(category, category_id)].append(
            (category_lvl1, category_lvl1_id))
        continue
      print('success: ' + category_lvl1)
      category_match = categories_table.query(f"category == '{category}' & category_lvl1 == '{category_lvl1}'")
      if len(category_match):
        internal_category_id = category_match.index[0]
      else:
        new_subcategory = {'category': category, 'category_lvl1': category_lvl1}
        categories_table = categories_table.append( new_subcategory, ignore_index=True)
        new_subcategory = {'category': [category], 'category_lvl1': [category_lvl1]}
        pd.DataFrame( new_subcategory).to_csv(base_dir + categories_fn,
                                          mode='a', header=False, index=False)
        internal_category_id = categories_table.index[-1]
      print('id: ' + str(internal_category_id))
      #converting the beautiful soup html into string to be able to pickle it
      subcat_products = [str(prod) for prod in scrape_subcategory(url)]
      subcat_products_html = {internal_category_id: subcat_products}
      with open( base_dir + products_html_fn, 'ab') as storage_file:
        pickle.dump( subcat_products_html, storage_file)
      successful_categories_lvl1[(category, category_id)].append(
          (category_lvl1, category_lvl1_id))
  else:
    failed_categories.append((category, category_id))

wd.quit()


s3 = boto3.resource('s3')
s3_obj = s3.Bucket(bucket_name).Object(bucket_key_base + str(date.today()))
s3_obj.upload_file(base_dir + products_html_fn)
