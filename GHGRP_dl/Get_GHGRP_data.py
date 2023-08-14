# -*- coding: utf-8 -*-
import json
import tempfile
from json import JSONDecodeError
from pathlib import Path

import pandas as pd
import requests
import xml.etree.ElementTree as et


def xml_to_df(xml_root, table_name, df_columns):
    """
    Converts elements of xml string obtained from EPA envirofact (GHGRP)
    to a DataFrame.
    """
    rpd = pd.DataFrame()

    for c in df_columns:
        cl = []
        for field in xml_root.findall(table_name):
            cl.append(field.find(c).text)
        cs = pd.Series(cl, name=c)
        rpd = pd.concat([rpd, cs], axis=1)

    return rpd


_GHGRP_records_cache = None


def get_GHGRP_records(
        reporting_year: int,
        table: str,
        rows: int = None,
        enable_cache: bool = True):
    """
    Return GHGRP data using EPA RESTful API based on specified reporting year 
    and table. Tables of interest are C_FUEL_LEVEL_INFORMATION, 
    D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and 
    V_GHG_EMITTER_FACILITIES.
    Optional argument to specify number of table rows.
    """

    print(f'Getting GHGRP records for {table}/{reporting_year}...')

    # TODO put cache in a dedicated class
    global _GHGRP_records_cache
    cache_key = f'{table}/{reporting_year}'

    tmp_dir = Path(Path(__file__).parent.parent, 'build', 'tmp')
    tmp_dir.mkdir(exist_ok=True)

    cache_file_path = Path(tmp_dir, 'ghgrp-records-cache.json')
    if enable_cache:
        if _GHGRP_records_cache is None:
            try:
                with open(cache_file_path, 'r') as cache_file:
                    _GHGRP_records_cache = json.loads(''.join(cache_file.readlines()))
                    print(f'Valid cache file found: {cache_file_path}')
            except (FileNotFoundError, JSONDecodeError) as e:
                print(f'No valid cache file found, will create one at {cache_file_path}')
                _GHGRP_records_cache = {}

        if cache_key in _GHGRP_records_cache:
            cached_result = pd.DataFrame.from_dict(_GHGRP_records_cache[cache_key])
            print(f'\tFound cached GHGRP records for {table}/{reporting_year} with {len(cached_result)} rows.')
            return cached_result

    # See https://www.epa.gov/enviro/envirofacts-data-service-api
    envirofacts_base_url = 'https://data.epa.gov/efservice'

    if table.startswith('V_GHG_EMITTER_'):
        table_url = f'{envirofacts_base_url}/{table}/YEAR/{reporting_year}'
    else:
        table_url = f'{envirofacts_base_url}/{table}/REPORTING_YEAR/{reporting_year}'

    r_columns = requests.get(f'{table_url}/rows/0:1')
    r_columns_root = et.fromstring(r_columns.content)

    clist = []

    for child in r_columns_root[0]:
        clist.append(child.tag)

    ghgrp = pd.DataFrame(columns=clist)

    if rows is None:
        try:
            r = requests.get(f'{table_url}/count/')
            n_records = int(list(et.fromstring(r.content).iter('TOTALQUERYRESULTS'))[0].text)
        except IndexError as ie:
            n_records = int(list(et.fromstring(r.content).iter('RequestRecordCount'))[0].text)
        except Exception as e:
            print(f'[ERROR] Encountered exception getting record count: {e}')
            r.raise_for_status()

        if n_records > 10000:
            # TODO implement proper pagination instead of this workaround
            r_range = range(0, n_records, 10000)

            for n in range(len(r_range) - 1):
                try:
                    r_records = requests.get(f'{table_url}/rows/{r_range[n]}:{r_range[n + 1]}')
                    records_root = et.fromstring(r_records.content)
                    r_df = xml_to_df(records_root, table, ghgrp.columns)
                    ghgrp = pd.concat([ghgrp, r_df])
                except Exception as e:
                    print(f'[ERROR] Encountered exception fetching or concatenating records: {e}')
                    r_records.raise_for_status()

            records_last = requests.get(f'{table_url}/rows/{r_range[-1]}:{n_records}')
            records_lroot = et.fromstring(records_last.content)
            rl_df = xml_to_df(records_lroot, table, ghgrp.columns)
            ghgrp = pd.concat([ghgrp, rl_df])

        else:
            try:
                r_records = requests.get(f'{table_url}/rows/0:{n_records}')
                records_root = et.fromstring(r_records.content)
                r_df = xml_to_df(records_root, table, ghgrp.columns)
                ghgrp = pd.concat([ghgrp, r_df])
            except Exception as e:
                print(f'[ERROR] Encountered exception fetching or concatenating records: {e}')
                r_records.raise_for_status()

    else:
        try:
            r_records = requests.get(f'{table_url}/rows/0:{rows}')
            records_root = et.fromstring(r_records.content)
            r_df = xml_to_df(records_root, table, ghgrp.columns)
            ghgrp = pd.concat([ghgrp, r_df])
        except Exception as e:
            print(f'[ERROR] Encountered exception fetching or concatenating records: {e}')
            r_records.raise_for_status()

    ghgrp.drop_duplicates(inplace=True)

    print(f'\tGot {len(ghgrp)} GHGRP records for {table}/{reporting_year}.')

    if enable_cache:
        _GHGRP_records_cache[cache_key] = ghgrp.to_dict()
        with open(cache_file_path, 'w') as cache_file:
            cache_file.write(json.dumps(_GHGRP_records_cache))

    return ghgrp


if __name__ == '__main__':
    for t in ['V_GHG_EMITTER_FACILITIES']:
        for year in [2015, 2021]:
            # TODO use logging (https://docs.python.org/3/library/logging.html) instead of print, here and elsewhere
            print(f'Getting data for {t}/{year}...')
            df = get_GHGRP_records(year, t)
            print(f'\tGot {len(df)} rows for {t}/{year}')

            csv_file_name = f'{t[0:7]}_{year}.csv'
            df.to_csv(csv_file_name, index=False)
            print(f'\tWrote {t}/{year} to {csv_file_name}')
