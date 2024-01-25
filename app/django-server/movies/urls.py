from django.urls import include, path
from movies.views import GetMoviesIdsView

urlpatterns = [
    path("filter-movie/", GetMoviesIdsView.as_view(), name="filter_movies"),
]
