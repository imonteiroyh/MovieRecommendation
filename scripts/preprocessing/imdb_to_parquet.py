from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


class IMDBDataTransformer:
    def __init__(self, data_directory: str = "data", chunk_size: int = 100_000):
        self.data_directory = Path(data_directory)
        self.chunk_size = chunk_size
        self.files = [
            "name_basics.tsv",
            "title_ratings.tsv",
            "title_basics.tsv",
            "title_crew.tsv",
            "title_principals.tsv",
            "title_akas.tsv",
        ]

        self.transform()

    def __preprocess_name_basics(self):
        self.current_chunk = self.current_chunk[
            ["nconst", "primaryName", "birthYear", "primaryProfession", "knownForTitles"]
        ]

        self.current_chunk = self.current_chunk.rename(
            {
                "nconst": "person_id",
                "primaryName": "name",
                "birthYear": "birth_year",
                "primaryProfession": "profession",
                "knownForTitles": "known_for_titles",
            },
            axis=1,
        )

    def __preprocess_title_ratings(self):
        self.current_chunk = self.current_chunk.rename(
            {
                "tconst": "title_id",
                "averageRating": "average_rating",
                "numVotes": "n_votes",
            },
            axis=1,
        )

    def __preprocess_title_basics(self):
        self.current_chunk["runtimeMinutes"] = pd.to_numeric(
            self.current_chunk["runtimeMinutes"], errors="coerce"
        )
        self.current_chunk = self.current_chunk[
            (self.current_chunk["titleType"] == "movie")
            & (self.current_chunk["isAdult"] == 0)
            & (self.current_chunk["primaryTitle"].notna())
        ]
        self.current_chunk = self.current_chunk[
            ["tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres"]
        ]

        self.current_chunk = self.current_chunk.rename(
            {
                "tconst": "title_id",
                "primaryTitle": "title",
                "startYear": "year",
                "runtimeMinutes": "runtime_minutes",
            },
            axis=1,
        )

        self.current_chunk = self.current_chunk.set_index(["title_id"])

    def __preprocess_title_crew(self):
        self.current_chunk = self.current_chunk.rename({"tconst": "title_id"}, axis=1)

        self.current_chunk = self.current_chunk.set_index(["title_id"])

    def __preprocess_title_principals(self):
        self.current_chunk = self.current_chunk[["tconst", "nconst"]]

        self.current_chunk = self.current_chunk.rename(
            {
                "tconst": "title_id",
                "nconst": "person_id",
            },
            axis=1,
        )

    def __preprocess_title_akas(self):
        self.current_chunk = self.current_chunk[["titleId", "title", "region"]]
        self.current_chunk = self.current_chunk[self.current_chunk["region"].isin(["BR", "US"])]

        self.current_chunk = self.current_chunk.rename(
            {
                "titleId": "title_id",
            },
            axis=1,
        )

        self.current_chunk = self.current_chunk.set_index(["title_id"])

    def __preprocess_chunk(self):
        file_handler_map = {
            "name_basics": self.__preprocess_name_basics,
            "title_ratings": self.__preprocess_title_ratings,
            "title_basics": self.__preprocess_title_basics,
            "title_crew": self.__preprocess_title_crew,
            "title_principals": self.__preprocess_title_principals,
            "title_akas": self.__preprocess_title_akas,
        }

        preprocess_function = file_handler_map.get(self.current_file, None)
        if preprocess_function is not None:
            preprocess_function()

    def transform(self):
        for file in self.files:
            self.current_file = Path(file).stem

            tsv_file = Path(self.data_directory, "raw", file)
            parquet_file = Path(self.data_directory, "interim", f"{self.current_file}.parquet")

            total_chunks = int(sum(1 for _ in open(tsv_file)) // self.chunk_size + 1)
            stream = pd.read_csv(
                tsv_file, sep="\t", chunksize=self.chunk_size, low_memory=False, na_values="\\N"
            )

            for index, chunk in tqdm(enumerate(stream), total=total_chunks, desc=f"Transforming {file}"):
                self.current_chunk = chunk
                self.__preprocess_chunk()

                if not index:
                    parquet_schema = pa.Table.from_pandas(self.current_chunk).schema
                    parquet_writer = pq.ParquetWriter(
                        parquet_file, schema=parquet_schema, compression="snappy"
                    )

                table = pa.Table.from_pandas(self.current_chunk, schema=parquet_schema)
                parquet_writer.write_table(table)

            parquet_writer.close()


IMDBDataTransformer()
