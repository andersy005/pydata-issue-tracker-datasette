# pydata-issue-tracker-datasette

[![collect-github-data](https://github.com/andersy005/pydata-issue-tracker-datasette/actions/workflows/github-to-sqlite.yaml/badge.svg)](https://github.com/andersy005/pydata-issue-tracker-datasette/actions/workflows/github-to-sqlite.yaml)

Github Action Workflow to pull [pydata/xarray](https://github.com/pydata/xarray)'s issue tracker data from Github's API
and build this dataset into a SQLite database, and serve the results using Datasette.

This workflow uses the following projects:

- https://github.com/simonw/csvs-to-sqlite
- https://github.com/simonw/datasette

Deployed to https://pydata-datasette.fly.dev/
