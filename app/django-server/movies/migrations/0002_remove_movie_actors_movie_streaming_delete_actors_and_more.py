# Generated by Django 5.0 on 2024-01-05 00:23

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("movies", "0001_initial"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="movie",
            name="actors",
        ),
        migrations.AddField(
            model_name="movie",
            name="streaming",
            field=models.ManyToManyField(related_name="streaming_provider", to="movies.providers"),
        ),
        migrations.DeleteModel(
            name="Actors",
        ),
        migrations.AddField(
            model_name="movie",
            name="actors",
            field=models.TextField(blank=True, null=True),
        ),
    ]
