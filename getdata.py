import requests
import os
import collections
import pandas as pd
import datetime
import pathlib


headers = {'Authorization': f"token {os.environ['GH_TOKEN']}"}


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


if __name__ == '__main__':

    now = datetime.datetime.now().replace(second=0, microsecond=0).timestamp()
    Project = collections.namedtuple('Project', ['org', 'repo'])
    data_file = pathlib.Path('./data/data.csv')

    projects = [
        Project('pydata', 'xarray'),
        Project('dask', 'dask'),
        Project('dask', 'distributed'),
        Project('numpy', 'numpy'),
        Project('pandas-dev', 'pandas'),
    ]
    states = [('OPEN', 'OPEN'), ('CLOSED', 'MERGED')]
    data = []
    for project in projects:
        entry = {'project': f'{project.org}/{project.repo}', 'time': now}
        for items in states:
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

    df = pd.DataFrame(data)

    try:
        old_df = pd.read_csv(data_file)
    except Exception:
        old_df = pd.DataFrame()

    print(f'Length of Old dataframe: {len(old_df)}')

    if old_df.empty:
        pass
    else:
        df = pd.concat([old_df, df])
    df = df.drop_duplicates(subset=['project', 'time']).sort_values(by='time')
    print(df.head())
    print(df.tail())
    print(f'Length of dataframe: {len(df)}')
    df.to_csv(data_file, index=False)
    print(f'File size in bytes: {data_file.stat().st_size}')
