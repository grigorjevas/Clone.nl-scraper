from bs4 import BeautifulSoup
import requests
import pandas as pd
import config
from psycopg2 import connect, OperationalError
from datetime import datetime


def connect_to_database():
    try:
        connection = connect(
            host=config.HOST,
            database=config.DB,
            user=config.USER,
            password=config.PASSWORD,
            port=config.PORT
        )
        return connection
    except OperationalError as err:
        print("pg error: ", err.pgerror, "\n")
        print("pg code: ", err.pgcode, "\n")


def execute_query(query: str) -> None:
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()


def execute_query_and_fetch(query: str) -> list:
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def drop_tables():
    execute_query('''
    DROP TABLE IF EXISTS categories CASCADE;
    DROP TABLE IF EXISTS items CASCADE;
    ''')


def create_tables():
    execute_query('''
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            category VARCHAR(64) UNIQUE);''')

    execute_query('''
        CREATE TABLE IF NOT EXISTS items (
            id SERIAL PRIMARY KEY,
            category_id INT,
            artist VARCHAR(64),
            title VARCHAR(255),
            label VARCHAR(255),
            format VARCHAR(64),
            price VARCHAR(8),
            item_url VARCHAR(255),
            thumb_url VARCHAR(255));''')

    execute_query('''
        ALTER TABLE items 
        ADD FOREIGN KEY (category_id) 
        REFERENCES categories(id);''')


def load_url(url: str):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup
    except requests.exceptions.Timeout:
        print("Connection timeout. Check your settings or try again.")
    except requests.exceptions.TooManyRedirects:
        print("Too many redirects. Check your URL.")
    except requests.exceptions.RequestException as e:
        print("Catastrophic error. Check your URL.")
        raise SystemExit(e)


def fetch_url(genre: str, number_of_examples: int) -> pd.DataFrame:
    artists, titles, labels, formats, prices, item_urls, thumb_urls = ([] for i in range(7))
    items_per_page = 50
    pages_to_fetch = int(number_of_examples / items_per_page)
    for i in range(1, pages_to_fetch+1):
        print(f"Fetching page {i}/{pages_to_fetch} from {genre} releases")
        url = f"https://clone.nl/instock/genre/{genre}?sort=id&order=desc&page={i}"
        soup = load_url(url)
        for artist in soup.select("div.description > h2 > a"):
            artists.append(artist.text.replace("'", "''"))
        for title in soup.select("div.description > h3 > a"):
            titles.append(title.text.replace("'", "''"))
        for label in soup.find_all("span", itemprop="recordLabel"):
            labels.append(label.text.replace("'", "''"))
        for media in soup.find_all("span", itemprop="material"):
            formats.append(media.text.replace("'", "''"))
        for price in soup.find_all("a", class_="addtocart"):
            prices.append(price.text.replace(" € ", "").replace(",", ".").replace("remind", "None"))
        for item_url in soup.select("div.description > h3 > a"):
            item_urls.append("https://clone.nl/" + item_url["href"])
        for thumb_url in soup.select("img.img-responsive"):
            thumb_urls.append(thumb_url["src"])

    records_data = list(zip(artists, titles, labels, formats, prices, item_urls, thumb_urls))
    return pd.DataFrame(records_data,
                        columns=["artists", "titles", "labels", "formats", "prices", "item_urls",
                                 "thumb_urls"])


def insert_dataframe_into_db(dataframe: pd.DataFrame, category: str) -> None:
    print("Inserting data into the database")
    execute_query(f'''
        INSERT INTO categories(category) 
        VALUES('{category}') 
        ON CONFLICT DO NOTHING''')
    category_id = execute_query_and_fetch(f"SELECT id FROM categories WHERE category = '{category}'")
    data_array = dataframe.to_records(index=False)
    query = ""
    for row in data_array:
        artist, title, label, media, price, item_url, thumb_url = row
        query += f'''
            INSERT INTO items(category_id, artist, title, label, format, price, item_url, thumb_url) 
            VALUES ('{category_id[0][0]}', '{artist}', '{title}', '{label}', '{media}', '{price}', 
            '{item_url}', '{thumb_url}');'''

    execute_query(query)


def export_to_csv():
    print("Exporting to csv")
    catalog = execute_query_and_fetch('''
        SELECT items.id, artist, title, label, category, format, price, item_url, thumb_url 
        FROM items 
        LEFT JOIN categories ON items.category_id = categories.id
        ORDER BY items.id''')
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    columns = ["id", "artist", "title", "label", "category", "format", "price", "item_url", "thumb_url"]
    catalog_df = pd.DataFrame(catalog)
    return catalog_df.to_csv(f"clone_nl_catalog_{timestamp}.csv", index=False, header=columns)
