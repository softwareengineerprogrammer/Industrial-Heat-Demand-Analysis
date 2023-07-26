# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 14:05:05 2017

@author: cmcmilla
"""

import pandas as pd
import numpy as np
import datetime
import requests
import xml.etree.ElementTree as et
import json

import Get_GHGRP_data
import GHGRP_energy_calc
import GHGRP_AAenergy_calc

if __name__ == '__main__':
    # years = range(2010, 2016)
    years = [2010, 2015, 2021]

    tables = [
        'c_fuel_level_information',
        'd_fuel_level_information',
        'V_GHG_EMITTER_FACILITIES',
        'v_ghg_emitter_subpart',
        'aa_fossil_fuel_information',
        'aa_spent_liquor_information',
    ]

    # Data on CO2 and CH4 emission factors by fuel type
    EFs = pd.read_csv('./EPA_FuelEFs.csv', index_col=['Fuel_Type'])
    EFs['dup_index'] = EFs.index.duplicated()
    EFs = pd.DataFrame(EFs[EFs.dup_index == False], columns=EFs.columns[0:2])

    # Set file directory
    file_dir = ''  # TODO use Path('.',<file_name>)

    # List of facilities for correction of combustion emissions from Wood and Wood
    # Residuals for using Subpart C Tier 4 calculation methodology.
    wood_facID = pd.read_csv('WoodRes_correction_facilities.csv', index_col=['FACILITY_ID'])

    ghgrp_energy = pd.DataFrame()
    aa_sl_table = pd.DataFrame()
    aa_ffuel_table = pd.DataFrame()

    # Get data from EPA API
    for y in years:
        for t in tables:
            df = Get_GHGRP_data.get_GHGRP_records(y, t)
            df.to_csv(f'{t[0:6]}_{y}.csv')

    for y in years:

        c_fuel_table = Get_GHGRP_data.get_GHGRP_records(y, tables[0])
        c_fuel_table.to_csv(f'c_fuel_{y}.csv')

        d_fuel_table = Get_GHGRP_data.get_GHGRP_records(y, tables[1])
        d_fuel_table.to_csv(f'd_fuel_{y}.csv')

        fac_table = Get_GHGRP_data.get_GHGRP_records(y, tables[2])
        fac_table.to_csv(f'fac_table_{y}.csv')

        for t in [('c_fuel_table', c_fuel_table), ('d_fuel_table', d_fuel_table), ('fac_table', fac_table)]:
            t[1].to_csv(f'{t[0]}_{y}.csv')

    # Subpart AA tables are much smaller and addressed slightly differently
    for y in years:
        aa_sl_table = pd.concat([
            aa_sl_table,
            Get_GHGRP_data.get_GHGRP_records(y, tables[5])
        ])

        aa_ffuel_table = pd.concat([
            aa_ffuel_table,
            Get_GHGRP_data.get_GHGRP_records(y, tables[4])
        ])

    aa_sl_file = f'aa_sl_table_{years[0]}{years[-1]}.csv'
    aa_sl_table.to_csv(file_dir + aa_sl_file)

    aa_ffuel_file = f'aa_ffuel_table_{years[0]}{years[-1]}.csv'
    aa_ffuel_table.to_csv(file_dir + aa_ffuel_file)

    facfile_2010 = file_dir + 'fac_table_2010.csv'
    facfiles_201115 = []

    for y in years:
        facfiles_201115.append(
            file_dir + f'fac_table_{y}.csv'
        )

    # Finding missing FIPS codes in format_GHGRP_facilities takes a long time due
    # to the way an API query is currently written.
    facdata = GHGRP_energy_calc.format_GHGRP_facilities(
        facfile_2010, facfiles_201115
    )

    for y in years:
        c_file = file_dir + f'c_fuel_{y}.csv'
        d_file = file_dir + f'd_fuel_{y}.csv'

        GHGs_y = \
            GHGRP_energy_calc.format_GHGRP_emissions(c_file, d_file)

        GHGs_y = GHGRP_energy_calc.calculate_energy(
            GHGs_y, facdata, EFs, wood_facID
        )

        ghgrp_energy = pd.concat([ghgrp_energy, GHGs_y])

    # Calculate energy for Subpart AA reporters
    GHGs_FF = GHGRP_AAenergy_calc.format_GHGRP_AAff_emissions(
        file_dir + aa_ffuel_file
    )

    GHGs_SL = GHGRP_AAenergy_calc.format_GHGRP_AAsl_emissions(
        file_dir + aa_sl_file
    )

    AA_FF_energy = GHGRP_AAenergy_calc.MMBTU_calc_AAff(GHGs_FF, EFs)
    AA_SL_energy = GHGRP_AAenergy_calc.MMBTU_calc_AAsl(GHGs_SL)
    AA_energy = GHGRP_AAenergy_calc.AA_merge(AA_FF_energy, AA_SL_energy)

    # Merge calculated energy values for Subparts AA, C, and D
    ghgrp_energy = GHGRP_energy_calc.energy_merge(ghgrp_energy, facdata, AA_energy)
    ghgrp_energy = GHGRP_energy_calc.id_industry_groups(ghgrp_energy)

    # Add timestamp to output csv file.
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M")

    outputfile = f'GHGRP_all_{ts}.csv'

    ghgrp_energy[[
        'CITY',
        'COUNTY',
        'COUNTY_FIPS',
        'ZIP',
        'FACILITY_ID',
        'FACILITY_NAME',
        'FUEL_TYPE',
        'FUEL_TYPE_BLEND',
        'FUEL_TYPE_OTHER',
        'GROUPING',
        'MECS_Region',
        'PARENT_COMPANY',
        'PNC_3',
        'PRIMARY_NAICS_CODE',
        'SECONDARY_NAICS_CODE',
        'ADDITIONAL_NAICS_CODES',
        'REPORTING_YEAR',
        'STATE',
        'STATE_NAME',
        'UNIT_NAME',
        'UNIT_TYPE',
        'MMBtu_TOTAL'
    ]].to_csv(file_dir + outputfile)
