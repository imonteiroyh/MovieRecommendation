# Project Movie Recomendation
## Pre - requisites

- Python 3.10

## .env Configuration
For configuring environment variables, create a .env file in the project's root directory. You can use the .env.example file as a reference. This file contains the necessary environment variables that your project requires. Rename the .env.example file to .env and fill in the appropriate values for your configuration.

## Installation

1. **Clone the repository**

[//]: # (    ```bash)

[//]: # (    git clone https://github.com/programando-com-edu/Movie-Recomendation.git)

[//]: # (    cd FlickPicks)

[//]: # (    ```)

2. **Create a Virtual Environment**

    It's always a good practice to use virtual environments to keep your project dependencies isolated. Create a virtual environment using Python 3.10.

    ```bash
    python3.10 -m venv venv
    ```

3. **Activate the Virtual Environment**

    On macOS and Linux:

    ```bash
    source venv/bin/activate
    ```

    On Windows:

    ```bash
    .\venv\Scripts\activate
    ```

4. **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```

5. **Enter Django Directory**

   Before start django server you need to enter in server directory.     

    ```bash
    cd django-server
    ```

6. **Database Setup**

    Run migrations to set up the initial database schema and run cities_import script for create cities table in database.

    ```bash
    python manage.py makemigrations
    python manage.py migrate
    python manage.py import_movie_data
    ```

7. **Run the Development Server**

    ```bash
    python manage.py runserver
    ```

    Your Django application will be accessible at `http://localhost:8000`.

