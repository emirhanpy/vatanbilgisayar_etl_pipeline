import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient

def iterate_page_count(url) -> int:
    """Page count is calculated"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/129.0.0.0 Safari/537.36'}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    page_count = soup.find('p', class_="wrapper-detailpage-header__text").text.split()
    for pc in page_count:
        if re.match("^[0-9]+$", pc):
            total_products = int(pc)
            page_count = (total_products // 24) + 1

    return page_count


def extract_from_category(url) -> dict:
    """It is extracted data from category such as links, codes, brands, names, prices, and review counts"""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/129.0.0.0 Safari/537.36'}
    page_count = iterate_page_count(url)

    product_links = []
    product_codes = []
    product_brands = []
    product_names = []
    product_prices = []
    product_review_counts = []

    for i in range(1, page_count + 1):
        current_url = f"{url}{i}"
        print(f"Scraping from page: {i}: {current_url}")
        r = requests.get(current_url, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        product_cards = soup.find_all('div', class_="product-list product-list--list-page")
        if not product_cards:
            print(f"No products found on page {i}.")
            continue

        product_links.extend(["https://www.vatanbilgisayar.com" + product_card.find('a')['href']
                              for product_card in product_cards])
        product_brands.extend([list(product_card.find('h3').text.split(" "))[0] for product_card in product_cards])
        product_names.extend([product_card.find('h3').text for product_card in product_cards])
        product_codes.extend(
            [product_card.find('div', class_="product-list__product-code").text.replace('\n', '').strip()
             for product_card in product_cards])
        product_prices.extend(
            [product_card.find('span', class_="product-list__price").text.replace('.', '').replace(',', '').strip(" ")
             if product_card.find('span', class_="product-list__price") else '0'
             for product_card in product_cards])
        product_review_counts.extend([product_card.find('a', class_="comment-count").text.replace('\n', '').strip("()")
                                      if product_card.find('a', class_="comment-count") else '0'
                                      for product_card in product_cards])

    scraped_data = {
        "LINK": product_links,
        "CODE": product_codes,
        "BRAND": product_brands,
        "NAME": product_names,
        "PRICE": product_prices,
        "REVIEWS": product_review_counts
    }

    return scraped_data

def transform_data(extracted_data) -> pd.DataFrame:
    """Transform the scraped data into a more readable and understandable format,
    and convert the data types of some columns. Then, different currency rates are added to the df."""

    df = pd.DataFrame(extracted_data)

    # Editing brand and name columns
    df['BRAND'] = df['BRAND'].str.upper()
    df['NAME'] = df['NAME'].str.lower()

    # Converting price and review count columns to integers
    df['PRICE'] = df['PRICE'].replace({'': '0'}).astype(int)
    df['REVIEWS'] = df['REVIEWS'].replace({'': '0'}).astype(int)

    # Adding currency conversion
    df['PRICE_TRY'] = df['PRICE']
    df['PRICE_EURO'] = (df['PRICE_TRY'] / 36.94).astype(int)
    df['PRICE_USD'] = (df['PRICE_TRY'] / 34.25).astype(int)

    return df

def load_to_csv(df):
    """This function load data into a CSV format."""

    df.to_csv('products.csv', index=False)
    print("Data successfully saved to CSV.")


def load_to_mongodb(df):
    """It loads the data to MongoDB and returns the collection for further querying."""

    data = df.to_dict('records')


    client = MongoClient('localhost', 27017)
    db = client['vatancomputer']
    collection = db['products']

    if data:
        collection.insert_many(data)
        print("Data successfully saved to MongoDB.")

    return collection, client

URL = 'https://www.vatanbilgisayar.com/bilgisayar/?page='
extract = extract_from_category(URL)
transform = transform_data(extract)
load_to_csv(transform)
load_to_mongodb(transform)
