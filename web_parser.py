import date
import boto3
import pickle
import pandas as pd
from collections import defaultdict
from bs4 import BeautifulSoup as soup

products_html_fn = 'products_html.pkl'
product_data_fn = 'product_data.csv'
categories_fn = 'product_categories.csv'
base_dir = "/home/ubuntu/scraping/"
parsed_products_fn = 'parsed_products.pkl'
bucket_name = 'continente-scraping'
bucket_key_base = 'products_parsed/catalogue-'


def prod_unpickler(fn):
  products = {}
  with open(fn, 'rb') as load_file:
    while True:
      try:
        new_prods = pickle.load(load_file)
      except EOFError:
        break
      products.update(new_prods)
  return products


def price_parser(raw_price):
  price, unit = raw_price.split()[1:]
  price = float(price.replace(',', '.'))
  return price, unit


def extract_product(container):
  # product description
  success = True
  product_info ={}
  description_html = container.findChild("div",{"class":"containerDescription"})
  try:
    product_info['product'] = description_html.div.a["title"]
  except AttributeError:
    success = False
  try:
    product_info['brand'] = description_html.find("div",{"class":"type"}).text
  except AttributeError:
    pass
  try:
    details = description_html.find("div",{"class":"subTitle"}).text
  except AttributeError:
    pass
  else:
    product_info['details'] = details.strip()
  # product price
  price_html = container.findChild("div",{"class":"containerPrice"})
  try:
    price = price_html.find("div",{"class":"priceFirstRow"}).text
  except AttributeError:
    success = False
  else:
    try:
      product_info['price'], product_info['price_units'] = price_parser(price)
    except:
      success = False
  try:
    price_weight = price_html.find("div",{"class":"priceSecondRow"}).text
  except AttributeError:
    try:
      product_info['price_weight'], product_info['price_weight_units'] = price_parser(price_weight)
    except:
      success = False
  discount_html = container.findChild("div",{"class": "discountMessage priceWas"})
  try:
    product_info['price_pack_no_discount'] = discount_html.findChild("div",{"class": "priceFirstRow"}).text
  except AttributeError:
    pass
  try:
    product_info['price_weight_no_discount'] = discount_html.findChild("div",{"class": "priceSecondRow"}).text
  except AttributeError:
    pass
  try:
    product_info['discount_text'] = discount_html.findChild("span",{"class": "iconDiscountText"}).text
  except AttributeError:
    pass
  prod_links = container.findChild('div', {'class': 'image'})
  try:
    product_info['image_url'] = prod_links.a.img['src']
  except AttributeError:
    pass
  try:
    product_info['url'] = prod_links.a['href']
  except AttributeError:
    pass
  return success, product_info


products_html = prod_unpickler(base_dir + products_html_fn)
categories = pd.read_csv( base_dir + categories_fn)


prods_not_parsed = defaultdict(list)
prods_parsed = []
for category_id, containers in products_html.items():
  i = 0
  for container in containers:
    success, prod = extract_product( soup(container))
    if success:
      prod['category_id'] = category_id
      prods_parsed.append(prod)
    else:
      prods_not_parsed[category_id].append(i)
    i += 1

with open(base_dir + parsed_products_fn, 'wb') as save_file:
  pickle.dump( prods_parsed, save_file)


s3 = boto3.resource('s3')
s3_obj = s3.Bucket(bucket_name).Object(bucket_key_base + str(date.today()))
s3_obj.upload_file(base_dir + prods_parsed)
