import requests
import os
import collections
import pandas as pd
import datetime
import itertools
import pathlib
from prefect import Flow, task, unmapped


headers = {'Authorization': f"token {os.environ['GH_TOKEN']}"}

@task
def get_repo_data(project, time):
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

        entry[f'{items[0].lower()}_issues'] = result['data']['repository'][
            'issues'
        ]['totalCount']
        entry[f'{items[1].lower()}_pull_requests'] = result['data']['repository'][
            'pullRequests'
        ]['totalCount']

        data.append(entry)
    return data


@task
def merge_data(data):
    entries = itertools.chain(*data)
    df = pd.DataFrame(entries)
    df['time'] = df.time.dt.round('H')
    df['hour'] = df.time.dt.hour
    df['day'] = df.time.dt.day
    df['week'] = df.time.dt.isocalendar().week
    df['weekend'] = df.time.dt.weekday.map(lambda x: 'weekday' if x < 5 else 'weekend')
    df['quarter'] = df.time.dt.quarter.map({1: 'Q1: Jan - Mar', 2: 'Q2: Apr - Jun', 3: 'Q3: Jul - Sep', 4: 'Q4: Oct - Dec'})
    return df


@task
def save_data(data, data_file):
    try:
        df = pd.read_json(data_file, parse_dates=['time'])
    except Exception:
        df = pd.DataFrame()


    if not data.empty:
        data = pd.concat([df, data])
    data = data.drop_duplicates(subset=['project', 'time']).sort_values(by='time')
    data.to_json(data_file, orient='records')


if __name__ == '__main__':

    with Flow('get_data') as flow:

        Project = collections.namedtuple('Project', ['org', 'repo'])
        projects = [
        Project('pydata', 'xarray'),
        Project('dask', 'dask'),
        Project('dask', 'distributed'),
        Project('numpy', 'numpy'),
        Project('pandas-dev', 'pandas'),
        Project('jupyterlab', 'jupyterlab'),
        Project('matplotlib', 'matplotlib'),
    ][:2]

        time =  datetime.datetime.now()
        data = get_repo_data.map(project=projects, time=unmapped(time))
        data = merge_data(data)
        save_data(data,data_file = pathlib.Path('./data/data.json').absolute())
    output = flow.run()
