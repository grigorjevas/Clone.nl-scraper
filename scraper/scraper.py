from bs4 import BeautifulSoup
import requests


def soup_init(url):
    print("Making soup")
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    return response

