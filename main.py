from typing import Tuple

from jellyfish import jaro_winkler_similarity as dist
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
import multiprocessing
import json

ABSACC = 0.95

locations = pd.read_csv('locations.csv')['location'].unique()
datacities = pd.read_csv('geonames-all-cities-with-a-population-1000.csv', sep=';')
datacities.sort_values('Population', inplace=True)
# countries = dict()
dicttowns = dict()
for name, alt, country in zip(datacities['Name'], datacities['Alternate Names'], datacities['Country name EN']):
    # townslst = list(set(str(alt).split(',')+[name]))
    # if country in countries:
    # countries[country] += townslst
    dicttowns[name] = country
    for alt_name in str(alt).split(','):
        dicttowns[alt_name] = country
countries = list(set(datacities['Country name EN']))


def finder(line: str) -> Tuple[str, str]:
    if line == 'Other':
        return 'Other'
    country = ('Other', 0)
    for i in countries:
        temp = (str(i), dist(line, str(i)))
        if temp[1] > ABSACC:
            return temp[0]
        if temp[1] > country[1]:
            country = temp

    town = ('Other', 0)
    for key, val in dicttowns.items():
        temp = (str(val), dist(line, str(key)))
        if temp[1] > ABSACC:
            return temp[0]
        if temp[1] > town[1]:
            town = temp

    result = max([country, town], key=lambda x: x[1])
    res = result[0] if result[1] >= 0.77 else 'Other'
    return line, res


if __name__ == '__main__':
    print(multiprocessing.cpu_count())
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as p:
        max_ = len(locations)
        result = dict()
        with tqdm(total=max_) as pbar:
            for i in p.imap_unordered(finder, locations):
                result[i[0]] = i[1]
                pbar.update()

        with open('locations.json', 'w') as fp:
            json.dump(result, fp)



