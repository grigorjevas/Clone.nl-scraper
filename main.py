from scraper import scraper


def main():
    # genres = ["techno", "house", "electronix"]
    # scraper.drop_tables()
    # scraper.create_tables()
    # for genre in genres:
    #     df = scraper.fetch_url(genre, 200)
    #     scraper.insert_dataframe_into_db(df, genre)
    scraper.export_to_csv()


if __name__ == '__main__':
    main()
