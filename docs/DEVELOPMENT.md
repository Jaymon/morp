# Development

## To work locally

```
$ git clone https://github.com/Jaymon/morp
$ cd morp
```

Optional:

```
$ python3 - venv .venv && source .venv/bin/activate
```

Install dependencies with pip `editable` mode

```
$ pip install -e .
```

### Setting up the interfaces

The dropfile interface works out of the box. You can test the PostgreSQL interface using Docker:

```
docker config up
```

You can set an `.env.local` file with any environment variables that the `docker-compose.yml` will pick up.

You can set up a local sqs using [elasticmq](https://github.com/softwaremill/elasticmq).

And set the appropriate DSN:

```
$ export MORP_DSN="sqs://x:x@?region=elasticmq&boto_endpoint_url=http://localhost:9324"
```

