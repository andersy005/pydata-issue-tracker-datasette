import collections
import datetime
import itertools
import os
import pathlib

import pandas as pd
import prefect
import requests

headers = {'Authorization': f"token {os.environ['GH_TOKEN']}"}


@prefect.task(retries=3, retry_delay_seconds=10)
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
    df['time'] = df.time.dt.round('H')
    df['hour'] = df.time.dt.hour
    df['day'] = df.time.dt.day
    df['week'] = df.time.dt.isocalendar().week
    df['weekend'] = df.time.dt.weekday.map(lambda x: 'weekday' if x < 5 else 'weekend')
    df['quarter'] = df.time.dt.quarter.map(
        {1: 'Q1: Jan - Mar', 2: 'Q2: Apr - Jun', 3: 'Q3: Jul - Sep', 4: 'Q4: Oct - Dec'}
    )
    return df


@prefect.task
def save_data(data, data_file):
    try:
        df = pd.read_json(data_file, convert_dates=['time'])
    except Exception:
        df = pd.DataFrame()
    print(df.shape)
    if not df.empty:
        data = pd.concat([df, data])
    data = data.drop_duplicates(subset=['project', 'time']).sort_values(by='time')
    with open(data_file, 'w', encoding='utf-8') as outfile:
        data.to_json(outfile, orient='records', indent=2, force_ascii=False)


def transform(row):
    return [
        {'date': row.time.round('D'), 'type': 'open_issues', 'count': row.open_issues},
        {
            'date': row.time.round('D'),
            'type': 'open_pull_requests',
            'count': row.open_pull_requests,
        },
    ]


@prefect.task
def save_project_weekly_data(project, path):
    path = pathlib.Path(path)
    columns = ['time', 'open_issues', 'open_pull_requests']
    df = pd.read_json(path, convert_dates=['time'])
    data = df[df.project == f'{project.org}/{project.repo}']
    data = data[columns].groupby(data.time.dt.isocalendar().week).last()
    results = pd.DataFrame(
        itertools.chain(*[transform(row) for _, row in data.iterrows()])
    )
    outdir = path.parent / 'weekly'
    outdir.mkdir(parents=True, exist_ok=True)
    data_file = f'{outdir}/{project.repo}-weekly-data.json'
    with open(data_file, 'w', encoding='utf-8') as outfile:
        results.to_json(outfile, orient='records', indent=2, force_ascii=False)


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
    path = pathlib.Path('./data/data.json').absolute()
    time = datetime.datetime.now()
    data = get_repo_data.map(project=projects, time=prefect.unmapped(time))
    df = merge_data(data)
    x = save_data(df, data_file=path)
    save_project_weekly_data.map(
        project=projects,
        path=prefect.unmapped(path),
        upstream_tasks=[prefect.unmapped(x)],
    )


if __name__ == '__main__':
    get_data()
