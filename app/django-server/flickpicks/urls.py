from django.contrib import admin
from django.urls import path, include
from movies import urls as movies


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include(movies)),

]
