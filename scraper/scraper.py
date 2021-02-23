from bs4 import BeautifulSoup
import requests
import pandas as pd


def load_page(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        return response
    except requests.exceptions.Timeout:
        print("Connection timeout. Check your settings or try again.")
    except requests.exceptions.TooManyRedirects:
        print("Too many redirects. Check your URL.")
    except requests.exceptions.RequestException as e:
        print("Catastrophic error. Check your URL.")
        raise SystemExit(e)


def fetch_data(keyword, number_of_examples):
    artists, titles, labels, formats, prices, item_urls, thumb_urls = ([] for i in range(7))
    items_per_page = 50
    pages_to_fetch = int(number_of_examples / items_per_page)
    for i in range(1, pages_to_fetch+1):
        response = load_page(f"https://clone.nl/instock/genre/{keyword}?sort=id&order=desc&page={i}")
        print(f"Parsing page {i}/{pages_to_fetch}... OK")
        soup = BeautifulSoup(response.content, "html.parser")
        for artist in soup.select("div.description > h2 > a"):
            artists.append(artist.text)
        for title in soup.select("div.description > h3 > a"):
            titles.append(title.text)
        for label in soup.find_all("span", itemprop="recordLabel"):
            labels.append(label.text)
        for media in soup.find_all("span", itemprop="material"):
            formats.append(media.text)
        for price in soup.find_all("a", class_="addtocart"):
            prices.append(price.text.replace(" € ", "").replace(",", "."))
        for item_url in soup.select("div.description > h3 > a"):
            item_urls.append(item_url["href"])
        for thumb_url in soup.select("img.img-responsive"):
            thumb_urls.append(thumb_url["src"])

    records_data = list(zip(artists, titles, labels, formats, prices, item_urls, thumb_urls))
    return pd.DataFrame(records_data,
                        columns=["artists", "titles", "labels", "formats", "prices", "item_urls",
                                 "thumb_urls"])

