from scraper.clone_nl import CloneScraper


def main():
    clone_scraper = CloneScraper()
    clone_scraper.drop_tables()
    clone_scraper.create_tables()
    genres = ["techno", "house", "electronix"]
    examples_to_scrape = 3000
    for genre in genres:
        df = clone_scraper.fetch_contents(genre, examples_to_scrape)
        clone_scraper.insert_data_into_db(df, genre)
    clone_scraper.export_to_csv()


if __name__ == '__main__':
    main()
