import json
import os
from pathlib import Path

import pandas as pd
import requests
import tmdbsimple as tmdb
import typer
from tqdm import tqdm

API_KEY = os.environ.get("TMDB_API_KEY")


class BaseFetcher:
    tmdb.API_KEY = API_KEY
    LANGUAGE = "pt-BR"
    REGION = "BR"

    def _write_to_file(self, file, data):
        if os.path.exists(file):
            data.to_csv(file, mode="a", header=False, index=False)
        else:
            data.to_csv(file, index=False)

    def run(self):
        pass


class TMDBTopRatedMoviesFetcher(BaseFetcher):
    MAX_PAGES = 1000

    def __init__(self, data_directory: str):
        self.write_file = Path(data_directory, "top_rated_movies.csv")
        self.movies = tmdb.Movies()

    def run(self):
        top = self.movies.top_rated(page=1, language=self.LANGUAGE, region=self.REGION)

        for page in tqdm(range(1, top["total_pages"] + 1), desc="Fetching top rated movies"):
            if page != 1:
                top = self.movies.top_rated(page=page, language=self.LANGUAGE, region=self.REGION)

            dataframe = pd.DataFrame(top["results"])
            self._write_to_file(self.write_file, dataframe)


class TMDBMovieProvidersFetcher(BaseFetcher):
    def __init__(self, data_directory: str):
        self.selected_titles_file = Path(data_directory, "top_rated_movies.csv")
        self.write_file = Path(data_directory, "movie_providers.csv")

    def __select_ids(self):
        selected_titles = pd.read_csv(self.selected_titles_file)
        self.selected_ids = selected_titles["id"].values

    def __fetch_movie_providers(self, id: int):
        movies = tmdb.Movies(id)
        providers = movies.watch_providers()

        data = []

        if self.REGION not in providers["results"]:
            return

        types = list(providers["results"][self.REGION].keys())
        types.remove("link")

        for transaction_type in types:
            for provider_data in providers["results"][self.REGION][transaction_type]:
                current_data = {
                    "id": providers["id"],
                    "link": providers["results"][self.REGION]["link"],
                    "transaction_type": transaction_type,
                    "provider_id": provider_data["provider_id"],
                }

                data.append(current_data)

        dataframe = pd.DataFrame(data)
        self._write_to_file(self.write_file, dataframe)

    def run(self):
        self.__select_ids()
        for id in tqdm(self.selected_ids, desc="Fetching movie providers"):
            self.__fetch_movie_providers(id)


class TMDBProvidersFetcher(BaseFetcher):
    def __init__(self, data_directory: str):
        self.write_file = Path(data_directory, "providers.csv")
        self.url = (
            "https://api.themoviedb.org/3/watch/providers/movie?"
            f"language={self.LANGUAGE}&watch_region={self.REGION}&api_key={API_KEY}"
        )

    def run(self):
        response = requests.get(self.url)
        result = json.loads(response.text)

        data = []

        for provider in tqdm(result["results"], desc="Fetching providers"):
            current_provider = {key: provider[key] for key in ["logo_path", "provider_name", "provider_id"]}

            data.append(current_provider)

        dataframe = pd.DataFrame(data)
        self._write_to_file(self.write_file, dataframe)


class TMDBGenresFetcher(BaseFetcher):
    def __init__(self, data_directory: str):
        self.write_file = Path(data_directory, "genres.csv")

    def run(self):
        with tqdm(total=1, desc="Fetching genres") as progress_bar:
            genres = tmdb.Genres()
            genres_list = genres.movie_list()
            dataframe = pd.DataFrame(genres_list["genres"])

            self._write_to_file(self.write_file, dataframe)

            progress_bar.update(1)


class TMDBMovieAdditionalInfoFetcher(BaseFetcher):
    def __init__(self, data_directory: str):
        self.selected_titles_file = Path(data_directory, "top_rated_movies.csv")
        self.additional_info_file = Path(data_directory, "additional_info.csv")

    def __select_ids(self):
        selected_titles = pd.read_csv(self.selected_titles_file)
        self.selected_ids = selected_titles["id"].values

    def __fetch_additional_info(self, id: int):
        movies = tmdb.Movies(id)
        info = movies.info()

        data = {
            "id": info["id"],
            "budget": info["budget"],
            "revenue": info["revenue"],
            "imdb_id": info["imdb_id"],
            "runtime": info["runtime"],
            "tagline": info["tagline"],
            "countries": ".".join([country["iso_3166_1"] for country in info["production_countries"]]),
        }

        dataframe = pd.DataFrame([data])
        self._write_to_file(self.additional_info_file, dataframe)

    def run(self):
        self.__select_ids()

        for id in tqdm(self.selected_ids, desc="Fetching movies additional info"):
            self.__fetch_additional_info(id)


class TMDBMovieKeywordsFetcher(BaseFetcher):
    def __init__(self, data_directory: str):
        self.selected_titles_file = Path(data_directory, "top_rated_movies.csv")
        self.keywords_file = Path(data_directory, "keywords.csv")

    def __select_ids(self):
        selected_titles = pd.read_csv(self.selected_titles_file)
        self.selected_ids = selected_titles["id"].values

    def __fetch_movies_keywords(self, id: int):
        keywords = tmdb.Movies(id).keywords()
        data = {"id": keywords["id"], "keywords": [keyword["name"] for keyword in keywords["keywords"]]}

        dataframe = pd.DataFrame([data])
        self._write_to_file(self.keywords_file, dataframe)

    def run(self):
        self.__select_ids()

        for id in tqdm(self.selected_ids, desc="Fetching movies keywords"):
            self.__fetch_movies_keywords(id)


def main(
    data_directory: str = typer.Option(
        help="Specify the data_directory to store the data.", default="data/raw/tmdb"
    )
):
    for fetcher in [
        TMDBTopRatedMoviesFetcher,
        TMDBMovieProvidersFetcher,
        TMDBProvidersFetcher,
        TMDBGenresFetcher,
        TMDBMovieAdditionalInfoFetcher,
        TMDBMovieKeywordsFetcher,
    ]:
        fetcher(data_directory).run()


if __name__ == "__main__":
    typer.run(main)
