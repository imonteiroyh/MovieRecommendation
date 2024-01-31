from movies.models import Movie
from rest_framework.permissions import AllowAny
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework.views import APIView

# Create your views here.


class GetMoviesIdsView(APIView):
    renderer_classes = [JSONRenderer]
    permission_classes = (AllowAny,)

    def filtered_ids(self, filters={}):
        try:
            movies_qs = Movie.objects.prefetch_related("genres", "streaming", "buy", "rent").filter(**filters)
            movies_ids = movies_qs.values_list("id", flat=True)
            return movies_ids
        except Exception as e:
            print(str(e))
            return []

    def post(self, request, *args, **kwargs):
        get_recommendations(self.filtered_ids(request.data))
        return Response(self.filtered_ids(request.data))


def get_recommendations(ids):
    pass
from django.shortcuts import render
from rest_framework.generics import RetrieveAPIView
from rest_framework.permissions import AllowAny
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework import status, generics
from rest_framework.views import APIView
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from movies.models import Movie


# Create your views here.


class GetMoviesIdsView(APIView):
    renderer_classes = [JSONRenderer]
    permission_classes = (AllowAny,)

    def filtered_ids(self, filters={}):
        try:
            movies_qs = Movie.objects.prefetch_related('genres', 'streaming', 'buy', 'rent').filter(**filters)
            movies_ids = movies_qs.values_list('id', flat=True)
            return movies_ids
        except Exception as e:
            print(str(e))
            return []

    def post(self, request, *args, **kwargs):
        get_recommendations(self.filtered_ids(request.data))
        return Response(self.filtered_ids(request.data))


def get_recommendations(ids, ignore_ids=None, weight_plot=0.7, n_movies=10):
    
    if ignore_ids is None:
        ignore_ids = ids
    else:
        ignore_ids.extend(ids)

    data = pd.read_parquet('data/processed/soup_data.parquet')
    count_vectorizer = CountVectorizer(stop_words="english")

    plot_matrix = count_vectorizer.fit_transform(data["soup_plot"])
    general_matrix = count_vectorizer.fit_transform(data["soup_general"])
    plot_similarity = cosine_similarity(plot_matrix, plot_matrix)
    general_similarity = cosine_similarity(general_matrix, general_matrix)

    result_similarity = weight_plot * plot_similarity + (1 - weight_plot) * general_similarity
    result = pd.DataFrame(result_similarity).iloc[ids]
    mean_result = result.mean(axis=0)
    sorted_result = mean_result.sort_values(ascending=False)
    real_result = sorted_result.drop(ignore_ids)
    recommended_ids = data.iloc[real_result.index[:n_movies]]['id'].tolist()

    return recommended_ids