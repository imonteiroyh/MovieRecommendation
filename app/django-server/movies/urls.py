from django.urls import path, include

from movies.views import GetMoviesIdsView

urlpatterns = [
    path('filter-movie/', GetMoviesIdsView.as_view(), name='filter_movies'),
]
