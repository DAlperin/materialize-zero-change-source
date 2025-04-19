# Materialize+Zero change source

## Getting Started

1. Copy `.env.example` to `.env` and fill in the required values.
   - The most interesting one is `MATERIALIZE_COLLECTIONS`, which is a comma-separated list of "collections" (read: views, matviews, tables, anything you can query in mz) that you want to sync to zero.
2. Run `docker-compose up` to start the services.

For an example of how to connect to this on the frontend, follow the instructions from the [client demo](https://github.com/DAlperin/materialize-zero-client-demo)
