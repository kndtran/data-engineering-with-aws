# Project: Data Warehouse

## Description

Sparkify, a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud.

This project creates a new ETL pipeline, moving data from S3 to a Redshift database.

## Schema Design

The Redshift database is designed using a STAR schema with a fact table and dimension tables. Using this schema, we can split artists, songs, and users into their own tables. This allows more efficient storage of values that don't change often, while the fact table can continue to record the song plays.
