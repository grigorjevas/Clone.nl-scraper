from scraper import database
from scraper import scraper
from bs4 import BeautifulSoup


if __name__ == '__main__':
    # scraper.fetch_data("techno", 100)
    # el_df = scraper.fetch_data("electronix", 100)
    # print(el_df)

    # create_categories = '''
    # CREATE TABLE categories (
    #     id SERIAL PRIMARY KEY,
    #     category VARCHAR(64)
    # )'''

    # create_items = '''
    # CREATE TABLE items (
    #     id SERIAL PRIMARY KEY,
    #     artist VARCHAR(64),
    #     title VARCHAR(255),
    #     label VARCHAR(255),
    #     price VARCHAR(8),
    #     item_url VARCHAR(255),
    #     thumb_url VARCHAR(255)
    # )'''

    # insert_category = "INSERT INTO categories(category) VALUES('techno');"
    #
    list_categories = "SELECT * FROM categories"
    #
    # database.execute_query(insert_category)
    rows = database.fetch_data(list_categories)
    # for row in rows:
    #     print(row)
