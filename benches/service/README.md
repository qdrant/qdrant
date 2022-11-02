# API K6 benchmarks

This directory contains benchmarks for the API.

Each benchmark is a separate javascript file, which is run by the corresponding shell script.

## Running the benchmarks

### upsert

The upsert benchmarks create a collection and upsert a number of documents into it.

The number of points and the vector's dimensions is configurable.

### search

The search benchmarks search for a random vector in a collection with a filter on an indexed payload field.

It runs for 1 minute with an increasing number of concurrent clients.

It is necessary to create a collection with the upsert benchmark before running the search benchmark.