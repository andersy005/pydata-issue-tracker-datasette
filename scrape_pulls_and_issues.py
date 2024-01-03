import collections
import datetime
import itertools
import logging
import os
import pathlib
import requests
import pandas as pd
import urllib.error

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
headers = {'Authorization': f"token {os.environ['GH_TOKEN']}"}

# Namedtuple for Project
Project = collections.namedtuple('Project', ['org', 'repo'])

def run_query(query, variables={}):
    """Function to make a GraphQL query to the GitHub API."""
    try:
        response = requests.post(
            'https://api.github.com/graphql',
            json={'query': query, 'variables': variables},
            headers=headers,
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
        return None

def get_repo_data(project, time):
    """Function to fetch repository data."""
    entry = {'project': f"{project.org}/{project.repo}", 'time': time}
    states = [('OPEN', 'OPEN'), ('CLOSED', 'MERGED')]
    data = []
    query = """
        query($org:String!, $repo:String!, $issue_state:[IssueState!], $pr_state:[PullRequestState!]){
            repository(owner: $org, name: $repo) {
                issues(states: $issue_state) {
                totalCount
                }
                pullRequests(states: $pr_state) {
                totalCount
                }
            }
        }"""

    for items in states:
        result = run_query(
            query,
            {
                'org': project.org,
                'repo': project.repo,
                'issue_state': items[0],
                'pr_state': items[1],
            },
        )
        if result is None:
            continue

        entry[f"{items[0].lower()}_issues"] = result['data']['repository']['issues']['totalCount']
        entry[f"{items[1].lower()}_pull_requests"] = result['data']['repository']['pullRequests']['totalCount']
        data.append(entry)

    return data

def merge_data(data):
    """Function to merge and process data."""
    try:
        df = pd.DataFrame.from_records(data)
        df['time'] = df.time.dt.round('D')
        df['day'] = df.time.dt.day
        df['week'] = df.time.dt.isocalendar().week
        df['weekend'] = df.time.dt.weekday.map(lambda x: 'weekday' if x < 5 else 'weekend')
        df['quarter'] = df.time.dt.quarter.map(
            {1: 'Q1: Jan - Mar', 2: 'Q2: Apr - Jun', 3: 'Q3: Jul - Sep', 4: 'Q4: Oct - Dec'}
        )
        return df
    except Exception as e:
        logging.error(f"Error in merging data: {e}")
        return pd.DataFrame()

def save_data(data, data_file):
    """Function to save data to a CSV file."""
    try:
        url = 'https://pydata-datasette.fly.dev/open_pulls_and_issues/open_pulls_and_issues.csv?_stream=on&_size=max'
        df = pd.read_csv(url, parse_dates=['time']).drop(columns=['rowid'])
    except urllib.error.HTTPError as e:
        logging.error(f"HTTP error occurred while fetching existing data: {e}")
        df = pd.DataFrame()
    except Exception as e:
        logging.error(f"Error occurred while fetching existing data: {e}")
        df = pd.DataFrame()

    logging.info(f"Existing data shape: {df.shape}")
    data = pd.concat([df, data])
    data = data.drop_duplicates(subset=['project', 'time']).sort_values(by='time')
    try:
        data.to_csv(data_file, index=False)
        logging.info(f"Data saved to {data_file}")
    except Exception as e:
        logging.error(f"Error saving data to CSV: {e}")

def get_data():
    """Main function to get data."""
    path = pathlib.Path('./data/open_pulls_and_issues.csv').absolute()
    time = datetime.datetime.now()

    data = get_repo_data(project=Project('pydata', 'xarray'), time=time)
    if data:
        df = merge_data(data)
        print(df)
        save_data(df, data_file=path)

if __name__ == '__main__':
    get_data()
