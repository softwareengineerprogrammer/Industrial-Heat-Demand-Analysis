# -*- coding: utf-8 -*-

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


def get_GHGRP_records(reporting_year: int, table: str, rows: int = None):
    """
    Return GHGRP data using EPA RESTful API based on specified reporting year 
    and table. Tables of interest are C_FUEL_LEVEL_INFORMATION, 
    D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and 
    V_GHG_EMITTER_FACILITIES.
    Optional argument to specify number of table rows.
    """

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
            n_records = int(et.fromstring(r.content)[0].text)
        except:
            r.raise_for_status()

        if n_records > 10000:
            r_range = range(0, n_records, 10000)

            for n in range(len(r_range) - 1):
                try:
                    r_records = requests.get(f'{table_url}/rows/{r_range[n]}:{r_range[n + 1]}')
                    records_root = et.fromstring(r_records.content)
                    r_df = xml_to_df(records_root, table, ghgrp.columns)
                    ghgrp = pd.concat([ghgrp, r_df])
                except:
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
            except:
                r_records.raise_for_status()

    else:
        try:
            r_records = requests.get(f'{table_url}/rows/0:{rows}')
            records_root = et.fromstring(r_records.content)
            r_df = xml_to_df(records_root, table, ghgrp.columns)
            ghgrp = pd.concat([ghgrp, r_df])
        except:
            r_records.raise_for_status()

    ghgrp.drop_duplicates(inplace=True)

    return ghgrp


if __name__ == '__main__':
    for t in ['V_GHG_EMITTER_FACILITIES']:
        for year in [2015, 2021]:
            print(f'Getting data for {t}/{year}...')
            df = get_GHGRP_records(year, t)
            print(f'\tGot {len(df)} rows for {t}/{year}')

            csv_file_name = f'{t[0:7]}_{year}.csv'
            df.to_csv(csv_file_name, index=False)
            print(f'\tWrote {t}/{year} to {csv_file_name}')
