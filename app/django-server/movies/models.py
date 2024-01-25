from django.db import models

# Create your models here.


class Providers(models.Model):
    logo_path = models.CharField(max_length=255, null=True, blank=True)
    provider_name = models.CharField(max_length=255, null=True, blank=True)
    provider_id = models.IntegerField(primary_key=True)


class Genre(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255)


class Movie(models.Model):
    id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=255)
    overview = models.TextField(null=True, blank=True)
    poster_path = models.CharField(max_length=255, null=True, blank=True)
    year = models.CharField(max_length=20, blank=True, null=True)
    runtime = models.CharField(max_length=255, blank=True, null=True)
    link = models.URLField(blank=True, null=True)
    free = models.BooleanField(default=False)
    actors = models.TextField(blank=True, null=True)
    genres = models.ManyToManyField(to=Genre)
    streaming = models.ManyToManyField(to=Providers, related_name="streaming_provider")
    buy = models.ManyToManyField(to=Providers, related_name="buy_provider")
    rent = models.ManyToManyField(to=Providers, related_name="rent_provider")
