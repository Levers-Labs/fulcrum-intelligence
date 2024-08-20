# story-manager

Insights Backend for user management in fulcrum intelligence

## Prerequisites

- `Python 3.10+`
- `Poetry 1.2+`
- `Postgresql 15+`

## Development

### `.env` example

```shell
ENV=dev
DEBUG=True
SERVER_HOST=http://localhost:8002
SECRET_KEY=qwtqwubYA0pN1GMmKsFKHMw_WCbboJvdTAgM9Fq
BACKEND_CORS_ORIGINS=["http://localhost"]
DATABASE_URL=postgres://postgres:password@localhost/fulcrum_db
```

### Initialize db

```shell
python manage.py db upgrade
```

### Run the fastapi app

```shell
python manage.py run-local-server
```

### Cli

There is a manage.py file at the root of the project, it contains a basic cli to hopefully
help you manage your project more easily. To get all available commands type this:

```shell
python manage.py --help
```
