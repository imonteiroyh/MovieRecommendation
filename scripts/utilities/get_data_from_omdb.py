import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm


class OMDBClient:
    def __init__(self, api_key, data_directory: str = "data", batch_size: int = 50):
        self.data_directory = Path(data_directory)
        self.api_key = api_key
        self.batch_size = batch_size

        self.requests = 0

        self.__setup_client()

    def __setup_client(self):
        self.relevant_titles_file = Path(self.data_directory, "processed", "omdb_relevant_titles.parquet")
        self.processed_titles_file = Path(self.data_directory, "interim", "omdb_processed_titles.csv")

        self.omdb_file = Path(self.data_directory, "raw", "omdb_data.csv")
        self.omdb_poster_directory = Path(self.data_directory, "raw", "omdb_posters")

        if not os.path.exists(self.processed_titles_file):
            dataframe = pd.DataFrame(columns=["title_id", "time"])
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
        relevant_titles = pd.read_parquet(self.relevant_titles_file)
        relevant_titles = relevant_titles[
            ~relevant_titles["title_id"].isin(self.processed_titles["title_id"])
        ]

        self.current_titles = relevant_titles.sample(self.batch_size)

    def __make_request(self, title_id: str):
        data_response = requests.get(f"http://www.omdbapi.com/?apikey={self.api_key}&i={title_id}&plot=full")
        self.requests += 1

        if data_response.status_code == 200:
            data = json.loads(data_response.text)

            poster = None

            if data["Response"] == "True":
                if data["Poster"].startswith("http"):
                    amazon_poster_response = requests.get(data["Poster"])

                    poster = (
                        amazon_poster_response.content if amazon_poster_response.status_code == 200 else None
                    )

                if poster is None:
                    api_poster_response = requests.get(
                        f"http://img.omdbapi.com/?apikey={self.api_key}&i={title_id}&h=10000"
                    )
                    self.requests += 1

                    poster = api_poster_response.content if api_poster_response.status_code == 200 else None

            result = {"data": data, "poster": poster}

            return result

        else:
            raise requests.exceptions.RequestException

    def __write_to_file(self):
        if len(self.write_info):
            titles = [{"title": id, "time": data["time"]} for id, data in self.write_info.items()]

            write_data = []
            for data in self.write_info.values():
                if data["data"]["Response"] == "True":
                    data["data"].update({"Time": data["time"]})
                    write_data.append(data["data"])

            poster_data = {
                id: data["poster"]
                for id, data in self.write_info.items()
                if (data["poster"] is not None) and (id in [data["title"] for data in titles])
            }

            omdb_data = pd.DataFrame(write_data)
            omdb_data.to_csv(self.omdb_file, mode="a", header=False, index=False)

            omdb_titles = pd.DataFrame(titles)
            omdb_titles.to_csv(self.processed_titles_file, mode="a", header=False, index=False)

            for id, poster in poster_data.items():
                poster_file = Path(self.omdb_poster_directory, id + ".jpg")
                with open(poster_file, "wb") as file:
                    file.write(poster)

            self.write_info = {}

    def run(self, requests_limit: int = 100):
        self.write_info = {}

        while self.requests < requests_limit:
            try:
                self.processed_titles = pd.read_csv(self.processed_titles_file)
                self.__select_current_titles()

                for title in tqdm(
                    self.current_titles.itertuples(),
                    desc=f"Downloading... Current requisition - {self.requests}",
                    total=self.batch_size,
                ):
                    current_title = title.title_id
                    result = self.__make_request(current_title)
                    result["time"] = datetime.now()
                    self.write_info[current_title] = result

                self.__write_to_file()

            except requests.exceptions.RequestException:
                self.__write_to_file()
                break


api_key = os.environ.get("OMDB_API_KEY")
client = OMDBClient(api_key=api_key)
client.run(requests_limit=10000)
