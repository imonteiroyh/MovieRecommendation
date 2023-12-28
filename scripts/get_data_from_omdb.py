import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import typer
from tqdm import tqdm


class OMDBClient:
    def __init__(self, api_key, data_directory: str = "data", batch_size: int = 50):
        self.data_directory = Path(data_directory)
        self.api_key = api_key
        self.batch_size = batch_size

        self.requests = 0

        self.__setup_client()

    def __setup_client(self):
        self.relevant_titles_file = Path(self.data_directory, "processed", "relevant_titles.parquet")
        self.processed_titles_file = Path(self.data_directory, "interim", "omdb_processed_titles.csv")

        self.omdb_file = Path(self.data_directory, "raw/omdb", "data.csv")

        self.relevant_titles = pd.read_parquet(self.relevant_titles_file)

        if not os.path.exists(self.processed_titles_file):
            dataframe = pd.DataFrame(columns=["imdb_id", "time"])
            dataframe.to_csv(self.processed_titles_file, index=False)

        if not os.path.exists(self.omdb_file):
            dataframe = pd.DataFrame(
                columns=[
                    "Title",
                    "Year",
                    "Rated",
                    "Released",
                    "Runtime",
                    "Genre",
                    "Director",
                    "Writer",
                    "Actors",
                    "Plot",
                    "Language",
                    "Country",
                    "Awards",
                    "Poster",
                    "Ratings",
                    "Metascore",
                    "imdbRating",
                    "imdbVotes",
                    "imdbID",
                    "Type",
                    "DVD",
                    "BoxOffice",
                    "Production",
                    "Website",
                    "Response",
                    "Time",
                ]
            )
            dataframe.to_csv(self.omdb_file, index=False)

    def __select_current_titles(self):
        self.relevant_titles = self.relevant_titles[
            ~self.relevant_titles["imdb_id"].isin(self.processed_titles["imdb_id"])
        ]

        self.current_titles = self.relevant_titles.sample(min(self.batch_size, len(self.relevant_titles)))

    def __make_request(self, imdb_id: str):
        data_response = requests.get(f"http://www.omdbapi.com/?apikey={self.api_key}&i={imdb_id}&plot=full")
        self.requests += 1

        if data_response.status_code == 200:
            data = json.loads(data_response.text)

            result = {"data": data}

            return result

        else:
            raise requests.exceptions.RequestException

    def __write_to_file(self):
        if len(self.write_info):
            titles = [{"title": id, "time": data["time"]} for id, data in self.write_info.items()]

            write_data = []
            write_titles = []
            for data in self.write_info.values():
                if data["data"]["Response"] == "True":
                    languages = data["data"]["Language"].split(", ")
                    type = data["data"]["Type"]
                    if type == "movie" and ("English" in languages or "Portuguese" in languages):
                        data["data"].update({"Time": data["time"]})
                        write_data.append(data["data"])
                        write_titles.append(data["data"]["imdbID"])

            omdb_data = pd.DataFrame(write_data)
            omdb_data.to_csv(self.omdb_file, mode="a", header=False, index=False)

            omdb_titles = pd.DataFrame(titles)
            omdb_titles.to_csv(self.processed_titles_file, mode="a", header=False, index=False)

            self.write_info = {}

    def run(self, requests_limit: int = 100):
        self.write_info = {}

        while self.requests < requests_limit and len(self.relevant_titles):
            try:
                self.processed_titles = pd.read_csv(self.processed_titles_file)
                self.__select_current_titles()

                for title in tqdm(
                    self.current_titles.itertuples(),
                    desc=f"Downloading... Current requisition - {self.requests}",
                    total=self.batch_size,
                ):
                    current_title = title.imdb_id
                    result = self.__make_request(current_title)
                    result["time"] = datetime.now()
                    self.write_info[current_title] = result

                self.__write_to_file()

            except requests.exceptions.RequestException:
                self.__write_to_file()
                break


def main():
    api_key = os.environ.get("OMDB_API_KEY")
    client = OMDBClient(api_key=api_key)
    client.run(requests_limit=10000)


if __name__ == "__main__":
    typer.run(main)
