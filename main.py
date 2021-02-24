from scraper.clone_nl import CloneScraper


def main():
    scraper = CloneScraper()
    scraper.drop_tables()
    scraper.create_tables()
    genres = ["techno", "house", "electronix"]
    examples_to_scrape = 3000
    for genre in genres:
        df = scraper.fetch_contents(genre, examples_to_scrape)
        scraper.insert_data_into_db(df, genre)
    scraper.export_to_csv()


if __name__ == '__main__':
    main()
