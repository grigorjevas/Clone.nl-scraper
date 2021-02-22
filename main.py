from scraper import database
from scraper import scraper
from bs4 import BeautifulSoup

if __name__ == '__main__':
    print("Let the fun begin")

    response = scraper.soup_init("https://clone.nl/instock/tag/electronix")
    print(response)

    soup = BeautifulSoup(response.content, "html.parser")
    for title in soup.select("div.description > h2 > a"):
        print(title.text)

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
    # list_categories = "SELECT * FROM categories"
    #
    # # database.execute_query(insert_category)
    # rows = database.fetch_data(list_categories)
    # for row in rows:
    #     print(row)
