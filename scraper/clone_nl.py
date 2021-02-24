from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import math
from scraper.database import Database

database = Database()


class CloneScraper:
    def __init__(self):
        """
        Clone.nl record shop scraper class.

        Available methods:
            * drop_tables
            * create_tables
            * fetch_contents
            * insert_data_into_db
            * export_to_csv
        """
        self.__items_per_page = 50

    @staticmethod
    def drop_tables() -> None:
        """
        Drops categories and items tables. Handle with care - this will destroy your data!
        """
        print("Dropping items and categories tables")
        database.execute_query('''
        DROP TABLE IF EXISTS categories CASCADE;
        DROP TABLE IF EXISTS items CASCADE;
        ''')

    @staticmethod
    def create_tables() -> None:
        """
        Creates categories and items tables and foreign keys.
        """
        print("Creating categories table")
        database.execute_query('''
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                category VARCHAR(64) UNIQUE);''')

        print("Creating items table")
        database.execute_query('''
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                category_id INT,
                artist VARCHAR(255),
                title VARCHAR(255),
                label VARCHAR(255),
                format VARCHAR(64),
                price VARCHAR(8),
                item_url VARCHAR(255),
                thumb_url VARCHAR(255));''')

        print("Setting up foreign keys")
        database.execute_query('''
            ALTER TABLE items 
            ADD FOREIGN KEY (category_id) 
            REFERENCES categories(id);''')

    def fetch_contents(self, genre: str, number_of_examples: int) -> pd.DataFrame:
        """
        Iterates through pages required to collect a specified number of examples. Minimum number of examples is 50.
        Uneven umber of examples will be rounded up to nearest 50.

        :param genre: str genre from Clone Records website. Available genres:
        Acid, Africa, Ambient, Bass, Belgium, Berlin, Boogie, Brazil, Breakbeat, Breaks,
        Chicago, Classical, Detroit, Disco, Drum & Bass, Drum and Bass, Dub, Dubstep, EBM,
        Electro, Electronix, Folk, Funk, Hardcore, Hip Hop, House, Indie, Italo, Italy, Jazz,
        Jungle, Library, Merchandise, Minimal, New York, Nordic, Outernational, Pop, Rave, Reggae,
        Rock, Soul, Soul Jazz, Soundtrack, Staff Pick Of The Week, Techno, Ticket, Trance, Tribal,
        Vintage, Wave

        :param number_of_examples: int number of assets to fetch. Assumes there are 50 items per page.
        :return: pandas DataFrame
        """
        genre = genre.lower()
        possible_genres = [
            "acid", "africa", "ambient", "bass", "belgium", "berlin", "boogie", "brazil", "breakbeat", "breaks",
            "chicago", "classical", "detroit", "disco", "drum", "&", "bass", "drum", "and", "bass", "dub", "dubstep",
            "ebmelectro", "electronix", "folk", "funk", "hardcore", "hip", "hop", "house", "indie", "italo", "italy",
            "jazzjungle", "library", "merchandise", "minimal", "new", "york", "nordic", "outernational", "pop", "rave",
            "reggaerock", "soul", "soul", "jazz", "soundtrack", "staff", "pick", "of", "the", "week", "techno",
            "ticket", "trance", "tribalvintage", "wave"
        ]

        if genre not in possible_genres:
            raise ValueError(f"Genre {genre} is not available at this shop.")

        if number_of_examples < 50:
            print("Minimum number of assets is 50.")
            number_of_examples = 50

        pages_to_fetch = math.ceil(number_of_examples / self.__items_per_page)
        artists, titles, labels, formats, prices, item_urls, thumb_urls = ([] for i in range(7))
        for i in range(1, pages_to_fetch + 1):
            print(f"Fetching page {i}/{pages_to_fetch} from {genre} releases")
            url = f"http://clone.nl/instock/genre/{genre}?sort=id&order=desc&page={i}"
            soup = CloneScraper.load_url(url)
            parser = CloneParser(soup)
            artists.extend(parser.parse_artists)
            titles.extend(parser.parse_titles)
            labels.extend(parser.parse_labels)
            formats.extend(parser.parse_formats)
            prices.extend(parser.parse_prices)
            item_urls.extend(parser.parse_item_urls)
            thumb_urls.extend(parser.parse_thumb_urls)

        records_data = list(zip(artists, titles, labels, formats, prices, item_urls, thumb_urls))
        return pd.DataFrame(records_data,
                            columns=["artists", "titles", "labels", "formats", "prices", "item_urls",
                                     "thumb_urls"])

    @staticmethod
    def load_url(url: str) -> BeautifulSoup:
        """
        Loads and parses an url using BeautifulSoup Python library.

        :param url: str, valid url, for example: https://turingcollege.com/
        :return: BeautifulSoup object
        """
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except requests.exceptions.Timeout as e:
            print("Connection timeout. Check your settings or try again.")
            raise ValueError(e)
        except requests.exceptions.TooManyRedirects as e:
            print("Too many redirects. Check your URL.")
            raise ValueError(e)
        except requests.exceptions.RequestException as e:
            print("Catastrophic error. Check your URL.")
            raise SystemError(e)

    @staticmethod
    def insert_data_into_db(dataframe: pd.DataFrame, category: str) -> None:
        """
        Inserts dataframe into the database.

        :param dataframe: pandas Dataframe
        :param category: category of the data
        """
        print("Inserting data into the database")
        database.execute_query(f'''
            INSERT INTO categories(category) 
            VALUES('{category}') 
            ON CONFLICT DO NOTHING''')
        category_id = database.execute_query_and_fetch(f"SELECT id FROM categories WHERE category = '{category}'")
        data_array = dataframe.to_records(index=False)
        query = ""
        for row in data_array:
            artist, title, label, media, price, item_url, thumb_url = row
            query += f'''
                INSERT INTO items(category_id, artist, title, label, format, price, item_url, thumb_url) 
                VALUES ('{category_id[0][0]}', '{artist}', '{title}', '{label}', '{media}', '{price}', 
                '{item_url}', '{thumb_url}');'''

        database.execute_query(query)

    @staticmethod
    def export_to_csv():
        """
        Connects to the database, fetches the data and exports the data to csv file.

        :return: csv file
        """
        print("Exporting to csv")
        catalog = database.execute_query_and_fetch('''
            SELECT items.id, artist, title, label, category, format, price, item_url, thumb_url 
            FROM items 
            LEFT JOIN categories ON items.category_id = categories.id
            ORDER BY items.id''')
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        columns = ["id", "artist", "title", "label", "category", "format", "price", "item_url", "thumb_url"]
        catalog_df = pd.DataFrame(catalog)
        catalog_df.to_csv(f"assets/clone_nl_catalog_{timestamp}.csv", index=False, header=columns)
        return


class CloneParser:
    def __init__(self, soup: object):
        """
        Parser class for Clone.nl record shop items.

        Available getters:
            * parse_artists
            * parse_titles
            * parse_labels
            * parse_formats
            * parse_prices
            * parse_item_urls
            * parse_thumb_urls

        :param soup: BeautifulSoup object
        """
        self.__soup = soup

    @property
    def parse_artists(self) -> list:
        return ([artist.text.replace("'", "''")
                    for artist in self.__soup.select("div.description > h2 > a")])

    @property
    def parse_titles(self) -> list:
        return ([title.text.replace("'", "''")
                for title in self.__soup.select("div.description > h3 > a")])

    @property
    def parse_labels(self) -> list:
        return ([label.text.replace("'", "''")
                for label in self.__soup.find_all("span", itemprop="recordLabel")])

    @property
    def parse_formats(self) -> list:
        return ([media.text.replace("'", "''")
                for media in self.__soup.find_all("span", itemprop="material")])

    @property
    def parse_prices(self) -> list:
        return ([price.text.replace(" € ", "").replace(",", ".").replace("remind", "None")
                for price in self.__soup.find_all("a", class_="addtocart")])

    @property
    def parse_item_urls(self) -> list:
        return (["https://clone.nl/" + item_url["href"]
                for item_url in self.__soup.select("div.description > h3 > a")])

    @property
    def parse_thumb_urls(self) -> list:
        return ([thumb_url["src"]
                for thumb_url in self.__soup.select("img.img-responsive")])


