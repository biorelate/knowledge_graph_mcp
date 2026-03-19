#!/bin/bash

export DBT_SOURCE_DATABASE="your_database"
export DBT_SOURCE_SCHEMA="raw"

dbt build
