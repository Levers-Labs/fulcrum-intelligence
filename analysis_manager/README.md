# analysis-manager
Analysis Manager for fulcrum intelligence

## Prerequisites

- `Python 3.9+`
- `Poetry 1.2+`
- `Postgresql 10+`


## Development

### `.env` example

```shell
ENV=dev
DEBUG=True
SERVER_HOST=http://localhost:8000
SECRET_KEY=qwtqwubYA0pN1GMmKsFKHMw_WCbboJvdTAgM9Fq
BACKEND_CORS_ORIGINS=["http://localhost"]
DATABASE_URL=postgres://postgres:password@localhost/fulcrum_db
REDIS_URL=redis://localhost
```

### Database setup

Create your first migration

```shell
aerich init-db
```

Adding new migrations.

```shell
aerich migrate --name <migration_name>
```

Upgrading the database when new migrations are created.

```shell
aerich upgrade
```

### Run the fastapi app

```shell
python manage.py work
```

### Cli

There is a manage.py file at the root of the project, it contains a basic cli to hopefully
help you manage your project more easily. To get all available commands type this:

```shell
python manage.py --help
```

