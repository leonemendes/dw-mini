# dw-mini

Mini Data Warehouse project.

## Setup

### Virtual Environment (VENV)

To create a venv, open the terminal and navigate to the project folder, then type:

```BASH
python3 -m venv .venv
```

To **activate** the virtual environment, type:

```BASH
source .venv/bin/activate
```

And then install packages with pip3.

```BASH
pip3 install -r requirements.txt
```

### Django

#### New project

```BASH
django-admin startproject backend .
```

#### New app

An app is a self-sufficient submodule of the project.

```BASH
python manage.py startapp app-name
```

#### Models

Models are source of information about your data. Django's ORM (Object-Relational-Mapper) allows interaction with the database using Python objects instead of raw SQL queries.

#### Tests

Django extends `unittest` framework to test the API.

```BASH
python manage.py test app-name
```

#### Migrations

Django migrations are a system for propagating changes made to your models (e.g., adding a field, deleting a model) into your database schema.

Two commands must be called: one to inspect and compare migrations files. It generates a new migration file containing declarative instructions on how to modify the database and reflect model changes; and one to aplly them to the database.

```BASH
python manage.py makemigrations core
python manage.py migrate
```

Django keeps track of which migrations have been applied in a special table called `django_migrations` within your database.

#### Run server

```BASH
python manage.py runserver
```

### Docker

Docker is an open-source platform that simplifies the process of building, deploying, and running applications using containers.
Containers are standardized, executable units that package an application's code along with all its dependencies, such as libraries, system tools, and configuration files. This ensures that the application runs consistently across different environments, from a developer's local machine to a production server.

The setup is made through a `docker-compose` file. It builds only infra-strucutre services used by the backend, so our django runs locally through venv and conects with the containers. At this file you can find:

* **postgres**: Relational database used for backend.
  * Env vars related to user credentials and db name.
  * Local port access at `5432`.
  * volume `postgres_data` to ensure data persistency.
* **redis**: Memory db used as cache and task queue.
  * Local port access at `6379`.
  * Used by Django as cache, Celery or even rate limiting.
* **clickhouse**: Analytics db optimized for large scale data reading.
  * Port `8123` (HTTP - can be tested through curl) and `9000`(TCP - for drivers).
  * Volume `clickhouse_data` for persistency.
* **minIO**: Object storage (S3 compatible).
  * Port `9000` (API) and `9001` (WEB console for object handling).
  * Env vars for credentials.
  * Volume `minio_data` to store objects.

To run docker deatached (in background, not blocking terminal) open Docker Daemon and run on terminal:

```BASH
docker-compose up -d
```

You can run an image separately by adding the image name at the very end of this same command.

To fetch containers status you can call

```BASH
docker ps
```

### .env

The `.env` file store environment variables and configuration settings for the project. This file aligns the env information set at `docker-compose` so the local backend can access the containerized part of the project.

## TroubleShooting

### At Setup

> Error: pg_config executable not found.

If this happens, probably you must install `PostgreSQL` from the web. If your macOS does not support it, you must install binaries from [PostgreSQL website](https://www.enterprisedb.com/download-postgresql-binaries) and run the following command lines under download folder:

```BASH
unzip postgresql-14.19-1-osx-binaries.zip -d ~/pgsql

export PATH="$HOME/pgsql/pgsql/bin:$PATH"
```

and then try to install requirements again at the same Terminal.
