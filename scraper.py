import os.path
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

JS = "https://angel.co/company_filters/search_data"

BASE_URL = "https://angel.co/companies/startups?ids%5B%5D={}&total={}&page={}&hexdigest={}"
DF_COLUMNS = ['name', 'desc', 'website', 'location', 'employees', 'raised', 'angel_url', 'angel_id']


HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'cookie': os.environ['ANGELCO_COOKIES'],
    'pragma': 'no-cache',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.142 Safari/537.36',
}
JS_HEADERS = {**HEADERS, "X-Requested-With": "XMLHttpRequest"}


def parse_companies(companies):
    df = pd.DataFrame(columns=DF_COLUMNS)
    for idx, company in enumerate(companies):
        name = company.findAll("a", {"class": "startup-link"})[1].text

        description = company.findAll("div", {"class": "pitch"})[0].text.strip('\n')
        if len(description) == 0:
            description = '-'

        company_column = company.findAll("div", {"class": "company column"})[0]
        angel_list_url = company_column.findAll('a', href=True)[0]['href']

        location_tag = company.findAll("div", {"class": "location"})[0]
        location = location_tag.findAll("div", {"class": "value"})[0].text.strip('\n')

        employees_tag = company.findAll("div", {"class": "company_size"})[0]
        employees = employees_tag.findAll("div", {"class": "value"})[0].text.strip('\n')

        raised_tag = company.findAll("div", {"class": "raised"})[0]
        raised = raised_tag.findAll("div", {"class": "value"})[0].text.strip('\n')

        website_tag = company.findAll("div", {"class": "website"})[0]
        a = website_tag.findAll('a', href=True)
        website = '-'
        if len(a) > 0:
            website = a[0]['href']

        angel_id = company.findAll("a", {"class": "startup-link"})[0]['data-id']
        full_company = pd.DataFrame([[name, description, angel_list_url, location,
                                      employees, raised, website, angel_id]], columns=DF_COLUMNS)

        df = df.append(full_company)
    return df


def get_next_pages(start_page=1, rang=(0, 100_000_000_000)):
    search_query = 'Startup'

    data = {
        "sort": "signal", "page": start_page,
        'filter_data[company_types][]': search_query,
        'filter_data[raised][min]': rang[0],
        'filter_data[raised][max]': rang[1],
    }
    with requests.Session() as s:
        response = s.post(JS, data=data, headers=JS_HEADERS)

        params = response.json()
        companies = s.get(BASE_URL.format("&ids%5B%5D=".join(map(str, params["ids"])), params["page"], params["total"], params["hexdigest"]),
                          headers=HEADERS)

        soup = BeautifulSoup(companies.json()["html"], "html.parser")
        companies = soup.findAll(name="div", attrs={"class": "base startup"})
        yield companies

        while True:
            page = params["page"] + 1

            params = s.post(JS, data={**data, "page": page}, headers=JS_HEADERS).json()

            if "ids" not in params or not params['ids']:
                break

            companies = s.get(BASE_URL.format("&ids%5B%5D=".join(map(str, params["ids"])), params["page"], params["total"], params["hexdigest"]),
                              headers=HEADERS)
            soup = BeautifulSoup(companies.json()["html"], "html.parser")
            companies = soup.findAll(name="div", attrs={"class": "base startup"})

            time.sleep(0.8)
            yield companies


def prepare_df(parsed_df):
    parsed_df = parsed_df.set_index('name')
    unique_companies = parsed_df.drop_duplicates()
    return unique_companies


def start(ranges):
    df = pd.DataFrame(columns=DF_COLUMNS)

    for rang in ranges:
        print("Getting range:", rang)

        companies = get_next_pages(rang=(rang[0], rang[1]))

        i = 0
        for idx, comps in enumerate(companies):
            parsed_companies = parse_companies(comps)
            print(f'Page {idx}, parsed companies:', len(parsed_companies))

            df = df.append(parsed_companies)
            i += len(parsed_companies)
        assert i == rang[2], (rang[2], i)

    return prepare_df(df)


def get_companies_count(rang):
    with requests.Session() as s:
        data = {
            'filter_data[company_types][]': 'Startup',
            'filter_data[raised][min]': rang[0],
            'filter_data[raised][max]': rang[1],
        }
        response = s.get(JS, data=data,
                         headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"})
        return int(response.json()['total'])


def get_distrib_points(init_range: tuple):
    tree = {}
    queue = [(init_range[0], init_range[1], None)]
    results = []
    i = 0
    while queue and (queue[-1][2] is None or queue[-1][2] > 400):
        i += 1

        q_item: tuple = queue.pop()
        rang: tuple = (q_item[0], q_item[1])
        res = get_companies_count(rang)
        time.sleep(1)
        print("Query res:", rang, res)

        results.append(rang + (res,))

        m = (rang[1] + rang[0]) // 2

        tree[rang] = {'left': (rang[0], m), 'right': (m, rang[1]), 'count': res}

        queue.insert(0, (rang[0], m, res))
        queue.insert(0, (m, rang[1], res))
        queue = sorted(queue, key=lambda x: x[2], reverse=False)

    return tree


def _traverse(t, node, res):
    if node not in t:
        print('kek')
        return
    if t[node]['left'] not in t:
        assert t[node]['right'] not in t

    if t[node]['right'] not in t:
        assert t[node]['left'] not in t

    assert (t[node]['right'] in t) == (t[node]['left'] in t)

    if t[node]['left'] not in t:
        res.append(node + (t[node]['count'],))
    else:
        _traverse(t, t[node]['left'], res)
        _traverse(t, t[node]['right'], res)


def main():
    initial_range = (1, 1_000_000)
    companies_tree = get_distrib_points(initial_range)
    tiles = []
    _traverse(companies_tree, initial_range, tiles)
    result_df = start(tiles)
    result_df.to_csv("final_raised_companies.csv")


if __name__ == "__main__":
    main()
