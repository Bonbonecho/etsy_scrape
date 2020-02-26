import requests
import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
import requests_cache
import re
import sqlalchemy

START_PAGE = 1
MAX_PAGE = 250
ETSY_DB = "sqlite:///../data/etsy.sqlite"

def make_etsy_request(page, base_url = "https://www.etsy.com/c/craft-supplies-and-tools/home-and-hobby"):
    """
    Make the request for an etsy page
    :param page: the page number
    :param base_url: the base url - defaults to craft supplies and tools
    :return: beautiful soup object
    """
    etsy_par = {"ref":"pagination","page":str(page)}
    etsy_req = requests.get(base_url,params=etsy_par)
    if etsy_req.status_code // 100 != 2:
        return None
    bs = BeautifulSoup(etsy_req.text,features="lxml")
    return bs


def proc_rating_num(num_string):
    nmat = re.search("\(([0-9,]+)\)",num_string)
    num_str = nmat.group(1)
    return float(num_str.replace(',',''))

def extract_card_info(li):
    """
    extracts relevant variables from etsy li tag
    :param li: tag from etsy page
    :return: dictionary of variable name : variable
    """
    card = li.find(class_="v2-listing-card__info")
    curren = card.find(class_="currency-symbol").text.strip()
    title = card.find("h2",class_="text-body").text.strip()
    value = card.find("span",class_="currency-value").text.strip()
    badges = card.find_all(class_="wt-badge")
    url = li.find("a",class_="listing-link")["href"]
    try:
        rating_num = card.find("span",class_="icon-b-1").text.strip()
        rating_num = proc_rating_num(rating_num)
    except AttributeError:
        rating_num = np.nan
    free_shipping, best_seller = False, False
    for badge in badges:
        if re.search("FREE shipping",badge.text):
            free_shipping = True
        if re.search("Bestseller",badge.text):
            best_seller = True
    # init_rating = float(card.find("input",attrs={"name":"initial-rating"})['value'])
    rating = card.find("input",attrs={"name":"rating"})
    if rating:
        rating = float(rating['value'])
    else:
        rating = np.nan
    shop = card.find(class_="v2-listing-card__shop").p.text.strip()
    return {"currency":curren, "title": title, "value":value, "free_shipping":free_shipping,
            "best_seller":best_seller,"rating":rating, "shop":shop, "url":url, "rating_num":rating_num}

if __name__=="__main__":
    ## Create the database, make queries, process data, and write to sqlite db
    requests_cache.install_cache("etsy_reqs")
    etsy_db = sqlalchemy.create_engine(ETSY_DB)
    for page in range(START_PAGE,MAX_PAGE+1):
        print(f"PROCESSING PAGE {page}...")
        etsy_r1 = make_etsy_request(page)
        if etsy_r1:
            wt_grid = etsy_r1.find("ul",class_="wt-grid")
            cards = pd.DataFrame(extract_card_info(li) for li in wt_grid if li.name=="li")
            cards.to_sql("cards",etsy_db,if_exists="append")
