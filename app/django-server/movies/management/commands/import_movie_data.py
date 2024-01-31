import os

import pandas as pd
from django.core.management.base import BaseCommand
from django.db import transaction

from ...models import Genre, Movie, Providers


class Command(BaseCommand):
    @staticmethod
    def _split_ids(data):
        return data.split(",") if data is not None else []

    @staticmethod
    def _create_providers(providers_data):
        _providers: list[Providers] = []
        for provider_data in providers_data.itertuples():
            if not Providers.objects.filter(pk=provider_data.provider_id).exists():
                provider_instance = Providers(
                    logo_path=provider_data.logo_path,
                    provider_name=provider_data.provider_name,
                    provider_id=provider_data.provider_id,
                )
                _providers.append(provider_instance)
        if _providers:
            with transaction.atomic():
                Providers.objects.bulk_create(_providers)

    @staticmethod
    def _create_genres(genres_data):
        _genres: list[Genre] = []
        for genre_data in genres_data.itertuples():
            if not Genre.objects.filter(pk=genre_data.id).exists():
                genre_instance = Genre(
                    id=genre_data.id,
                    name=genre_data.name,
                )
                _genres.append(genre_instance)
        if _genres:
            with transaction.atomic():
                Genre.objects.bulk_create(_genres)

    def _create_movies(self, movies_data):
        count = 0
        total_movies = len(movies_data)
        for movie_data in movies_data.itertuples():
            try:
                genre_ids = self._split_ids(movie_data.genre_ids)
                rent_ids = self._split_ids(movie_data.rent)
                flatrate_ids = self._split_ids(movie_data.flatrate)
                buy_ids = self._split_ids(movie_data.buy)
                genres = Genre.objects.filter(id__in=genre_ids or [])
                rent_providers = Providers.objects.filter(provider_id__in=rent_ids or [])
                streaming_providers = Providers.objects.filter(provider_id__in=flatrate_ids or [])
                buy_providers = Providers.objects.filter(provider_id__in=buy_ids or [])
                if movie_data.id is not None or movie_data.title is not None:
                    instance, created = Movie.objects.get_or_create(
                        id=movie_data.id,
                        title=movie_data.title,
                        overview=movie_data.overview,
                        poster_path=movie_data.poster_path,
                        year=movie_data.year,
                        runtime=movie_data.runtime,
                        actors=movie_data.actors,
                        free=movie_data.free
                        if movie_data.free is not None and isinstance(movie_data.free, bool)
                        else False,
                        link=movie_data.link,
                    )
                    [instance.genres.add(genre) for genre in genres]
                    [instance.streaming.add(streaming) for streaming in streaming_providers]
                    [instance.buy.add(buy) for buy in buy_providers]
                    [instance.rent.add(rent) for rent in rent_providers]

                    instance.save()
                    count += 1
                    if count % 100 == 0:
                        print(f"saving {count} records of {total_movies}")

            except Exception as e:
                print(str(e), f"Error in data{movie_data}")
        print("Task finish")

    def handle(self, *args, **options):
        from django.conf import settings

        movie_data_path = os.path.join(settings.STATIC_DIR, "app_data.parquet")
        genres_data_path = os.path.join(settings.STATIC_DIR, "genres.parquet")
        providers_data_path = os.path.join(settings.STATIC_DIR, "providers.parquet")
        movies_data = pd.read_parquet(movie_data_path)
        genres_data = pd.read_parquet(genres_data_path)
        providers_data = pd.read_parquet(providers_data_path)

        self._create_providers(providers_data)
        self._create_genres(genres_data)
        self._create_movies(movies_data)
