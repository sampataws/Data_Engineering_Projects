import re
import pandas as pd
import logging
from pathlib import Path


project_home = Path(__file__).resolve().parents[1].parent.parent.parent.parent
source_base_data_path = '{}/data'.format(project_home)

def extract_codes_from_sas_file_and_convert_csv(*args, **kwargs):
    """
    Grabs the codes from SAS data and convert to CSV.
    """

    sas_file = "{}/I94_SAS_Labels_Descriptions.sas".format(source_base_data_path)
    with open(sas_file, "r") as f:
        file = f.read()

    sasfile_dict = {}
    temp_data = []
    for line in file.split("\n"):
        line = re.sub(r"\s+", " ", line)
        if "/*" in line and "-" in line:
            k, v = [i.strip(" ") for i in line.split("*")[1]
                                              .split("-", 1)]
            k = k.replace(' & ', '_').lower()
            sasfile_dict[k] = {'description': v}
        elif '=' in line and ';' not in line:
            temp_data.append([i.strip(' ').strip("'").title()
                              for i in line.split('=')])
        elif len(temp_data) > 0:
            sasfile_dict[k]['data'] = temp_data
            temp_data = []

    sasfile_dict['i94cit_i94res']['df'] = pd.DataFrame(
        sasfile_dict['i94cit_i94res']['data'], columns=['code', 'country'])

    tempdf = pd.DataFrame(sasfile_dict['i94port']['data'],
                          columns=['code', 'port_of_entry'])
    tempdf['code'] = tempdf['code'].str.upper()
    tempdf[['city', 'state_or_country']] = tempdf['port_of_entry'
                                                  ].str.rsplit(',', 1,
                                                               expand=True)
    sasfile_dict['i94port']['df'] = tempdf

    sasfile_dict['i94mode']['df'] = pd.DataFrame(
        sasfile_dict['i94mode']['data'], columns=['code', 'transportation'])

    tempdf = pd.DataFrame(sasfile_dict['i94addr']['data'],
                          columns=['code', 'state'])
    tempdf['code'] = tempdf['code'].str.upper()
    sasfile_dict['i94addr']['df'] = tempdf

    sasfile_dict['i94visa']['df'] = pd.DataFrame(
        sasfile_dict['i94visa']['data'], columns=['code', 'reason_for_travel'])

    for table in sasfile_dict.keys():
        if 'df' in sasfile_dict[table].keys():
            logging.info(f"Writing {table} to S3")
            file = "{}/raw/codes/{}.csv".format(source_base_data_path,table)
            with open(file, "w") as f:
                sasfile_dict[table]['df'].to_csv(f, index=False)


extract_codes_from_sas_file_and_convert_csv()