### How to run

1. [Install poetry](https://python-poetry.org/docs/#installation)
2. [Install docker-compose](https://docs.docker.com/compose/install/)
3. Install python dependencies: `poetry install`
4. Start the database: `docker-compose up -d`
5. Ingest data: `python ingest_data.py`
6. Run example query: `python run_query.py`

The result will be:

```
        Bounding Box: -10, -10, 50, 90
        Average weekly trips: 7.8
```

### Scalability

The ingestion solution uses Dask and Dask Geopandas to make a scalable data ingestion pipeline. Dask allows for a distributed data ingestion system.

The database per-se is scalable as [PostGIS](https://postgis.net/) uses spatial indexing to optimize the spatial querying of data.

### Progression

During ingestion, a progress bar is provided to allow the user to follow the ingestion process.
