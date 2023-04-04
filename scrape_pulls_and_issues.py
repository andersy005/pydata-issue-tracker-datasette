import collections
import datetime
import itertools
import os
import pathlib
import urllib.error

import pandas as pd
import prefect
import requests

headers = {'Authorization': f"token {os.environ['GH_TOKEN']}"}


@prefect.task(
    retries=3,
    retry_delay_seconds=10,
)
def get_repo_data(project, time):  # sourcery skip: raise-specific-error
    def run_query(
        query, variables={}
    ):  # A simple function to use requests.post to make the API call. Note the json= section.
        request = requests.post(
            'https://api.github.com/graphql',
            json={'query': query, 'variables': variables},
            headers=headers,
        )
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(
                'Query failed to run by returning code of {}. {}'.format(
                    request.status_code, query
                )
            )

    entry = {'project': f'{project.org}/{project.repo}', 'time': time}
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
            variables={
                'org': project.org,
                'repo': project.repo,
                'issue_state': items[0],
                'pr_state': items[1],
            },
        )

        entry[f'{items[0].lower()}_issues'] = result['data']['repository']['issues'][
            'totalCount'
        ]
        entry[f'{items[1].lower()}_pull_requests'] = result['data']['repository'][
            'pullRequests'
        ]['totalCount']

        data.append(entry)
    return data


@prefect.task
def merge_data(data):
    entries = itertools.chain(*data)
    df = pd.DataFrame(entries)
    df['time'] = df.time.dt.round('D')
    df['day'] = df.time.dt.day
    df['week'] = df.time.dt.isocalendar().week
    df['weekend'] = df.time.dt.weekday.map(lambda x: 'weekday' if x < 5 else 'weekend')
    df['quarter'] = df.time.dt.quarter.map(
        {1: 'Q1: Jan - Mar', 2: 'Q2: Apr - Jun', 3: 'Q3: Jul - Sep', 4: 'Q4: Oct - Dec'}
    )
    return df


@prefect.task(
    retries=3,
    retry_delay_seconds=10,
)
def save_data(data, data_file):
    try:
        url = 'https://pydata-datasette.fly.dev/open_pulls_and_issues/open_pulls_and_issues.csv?_stream=on&_size=max'
        df = pd.read_csv(url, parse_dates=['time']).drop(columns=['rowid'])
    except urllib.error.HTTPError:
        df = pd.DataFrame()
    print(df.shape)
    if not df.empty:
        data = pd.concat([df, data])
        data = data.drop_duplicates(subset=['project', 'time']).sort_values(by='time')
        data.to_csv(data_file, index=False)


@prefect.flow
def get_data():

    Project = collections.namedtuple('Project', ['org', 'repo'])
    projects = [
        Project('pydata', 'xarray'),
        Project('dask', 'dask'),
        Project('dask', 'distributed'),
        Project('numpy', 'numpy'),
        Project('pandas-dev', 'pandas'),
        Project('jupyterlab', 'jupyterlab'),
        Project('matplotlib', 'matplotlib'),
    ]
    path = pathlib.Path('./data/open_pulls_and_issues.csv').absolute()
    time = datetime.datetime.now()
    data = get_repo_data.map(project=projects, time=prefect.unmapped(time))
    df = merge_data(data)
    save_data(df, data_file=path)
    save_data(df, data_file=path)


if __name__ == '__main__':
    get_data()
