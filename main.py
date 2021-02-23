from scraper import scraper


def main():
    scraper.drop_tables()
    scraper.create_tables()
    genres = ["techno", "house", "electronix"]
    for genre in genres:
        df = scraper.fetch_url(genre, 3000)
        scraper.insert_dataframe_into_db(df, genre)
    scraper.export_to_csv()


if __name__ == '__main__':
    main()
