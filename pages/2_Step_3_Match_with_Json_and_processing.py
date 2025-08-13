import os
import re
import ast
import numpy as np
import pandas as pd
import requests
import streamlit as st
from io import BytesIO

st.set_page_config(page_title="OFAC Data Processing and Export")

st.title("OFAC Data Processing and Export")

# Upload required file
uploaded_df_checkv2 = st.file_uploader("Upload df_checkv2.xlsx (required)", type=["xlsx"])

# Button to load local files instead of uploading
st.markdown("#### **Load Country_code.xlsx and OFAC_format.xlsx from online source or upload manually**")
use_local_others = st.button("click to Load Both Files")

# Optional upload for other two files
uploaded_country_code = st.file_uploader("Upload Country_code.xlsx (optional)", type=["xlsx"])
uploaded_df_format = st.file_uploader("Upload OFAC_format.xlsx (optional)", type=["xlsx"])




if uploaded_df_checkv2:

    # Read uploaded df_checkv2
    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text("Reading df_checkv2.xlsx...")
    df_checkv2 = pd.read_excel(uploaded_df_checkv2, dtype={'id': 'Int64'})
    progress_bar.progress(10)

    # Load Country_code.xlsx and OFAC_format.xlsx either from upload or local
    if use_local_others:
        try:
            country_code_df = pd.read_excel('Country_code.xlsx',keep_default_na=False, na_values=[])
            df_format = pd.read_excel('OFAC_format.xlsx')
            st.success("Loaded Country_code.xlsx and OFAC_format.xlsx from local files.")
        except Exception as e:
            st.error(f"Error loading local files: {e}")
            st.stop()
    else:
        if uploaded_country_code is not None and uploaded_df_format is not None:
            country_code_df = pd.read_excel(uploaded_country_code,keep_default_na=False, na_values=[])
            df_format = pd.read_excel(uploaded_df_format)
            st.success("Loaded Country_code.xlsx and OFAC_format.xlsx from uploads.")
        else:
            st.warning("Please upload Country_code.xlsx and OFAC_format.xlsx OR click the button to load them locally.")
            st.stop()

    progress_bar.progress(20)
    status_text.text("Fetching JSON data from online source...")
    url = "https://data.trade.gov/downloadable_consolidated_screening_list/v1/consolidated.json"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    df_json = pd.json_normalize(data, "results")
    progress_bar.progress(30)

    status_text.text("Combining and filtering data...")
    df_checkv2["id-name"] = df_checkv2["id"].astype("string").fillna('') + "-" + df_checkv2["name"].fillna('').astype(str)
    df_json["ID-NAME"] = df_json["id"] + "-" + df_json["name"]
    filtered_df = df_json[df_json['ID-NAME'].isin(df_checkv2['id-name'])]
    progress_bar.progress(40)

    status_text.text("Mapping additional columns...")
    name_to_source = dict(zip(df_checkv2['id-name'], df_checkv2['source']))
    idname_to_alias = dict(zip(df_checkv2['id-name'], df_checkv2['alias']))
    filtered_df['source'] = filtered_df['ID-NAME'].map(name_to_source)
    filtered_df['alias2'] = filtered_df['ID-NAME'].map(idname_to_alias)
    filtered_df2 = filtered_df.drop_duplicates(subset=['ID-NAME'], keep='last')
    progress_bar.progress(50)

    filtered_df2['alias2'] = filtered_df2['alias2'].fillna('').apply(lambda x: [item.strip() for item in x.split(';') if item.strip()])
    def merge_alt_into_alias(row):
        alt_names = row['alt_names']  # plain names
        alias2 = row['alias2']        # may contain a.k.a. / f.k.a.

        # Create a set of stripped alias2 names for duplicate checking
        stripped_alias = set()
        for a in alias2:
            stripped = re.sub(r'^(a\.k\.a\.|f\.k\.a\.)\s*', '', a, flags=re.I).strip()
            stripped_alias.add(stripped)

        # Add alt_names only if not in stripped_alias
        for name in alt_names:
            if name not in stripped_alias:
                alias2.append(name)  # add the plain name

        return alias2

    filtered_df2['alt_names'] = filtered_df2.apply(merge_alt_into_alias, axis=1)
    # filtered_df2.drop(columns=['alias2'], inplace=True)
    
    filtered_df2.drop(columns=['alias2'], inplace=True)
    temp_df = filtered_df2[['ID-NAME','entity_number', 'type','name','alt_names',
                            'places_of_birth','addresses','source', 'programs',                
                            'remarks','vessel_type','vessel_flag','vessel_owner',
                            'citizenships','dates_of_birth','country','nationalities','ids']]
    temp_df['remarks'] = temp_df['remarks'].fillna('').str.split(';')
    temp_df = temp_df.reset_index(drop=True)
    progress_bar.progress(60)
    status_text.text("Extracting sanction dates and order IDs...")

    temp_df['sanction_date'] = temp_df['source'].str.rsplit('/', n=1).str[-1].str[:8]

    def extract_order_ids(program_list):
        pattern = r'EO\d+'
        matches = []
        for item in program_list:
            if item:
                matches.extend(re.findall(pattern, item))
        matches = [m.replace('EO', 'E.O.') for m in matches]
        return "; ".join(matches)

    temp_df['order_id'] = temp_df['programs'].apply(extract_order_ids)
    temp_df['programs'] = temp_df['programs'].apply(
        lambda x: '; '.join(str(i) for i in x if i is not None) if isinstance(x, list) else x
    )

    def parse_ids_column(val):
        if isinstance(val, str):
            try:
                return ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return []
        return val

    temp_df['ids'] = temp_df['ids'].apply(parse_ids_column)

    status_text.text("Processing IDs and extracting required fields...")
    wanted_types = [
        'Phone Number',
        'Target Type',
        'Website',
        'Organization Established Date',
        'Organization Type:',
        'Email Address',
        'Gender'
    ]

    eo_patterns = [
        'Executive Order 14024',
        'Executive Order 13662',
        'Executive Order 13846'
    ]

    remove_types = [
        'Additional Sanctions Information -',
        'Effective Date (CMIC)',
        'Effective Date (EO 14024 Directive 1a):',
        'Effective Date (EO 14024 Directive 3):',
        'Effective Date (EO 14024 Directive 2):',
        'Secondary sanctions risk:',
        'Transactions Prohibited For Persons Owned or Controlled By U.S. Financial Institutions:',
        'Issuer Name'
    ] + wanted_types

    remove_types_lower = [t.lower().strip().rstrip(':') for t in remove_types]

    def process_ids(row):
        ids_list = row.get('ids')
        if not isinstance(ids_list, list):
            data = {f"{k}_extracted": None for k in wanted_types}
            data['order_id_extracted'] = row.get('order_id', None)
            data['ids_extracted'] = ids_list
            return pd.Series(data)

        extracted_values = {f"{k}_extracted": None for k in wanted_types}
        order_ids = []

        for d in ids_list:
            t = d.get('type')
            n = d.get('number')

            if t and t in wanted_types:
                extracted_values[f"{t}_extracted"] = n

            if t and any(pattern in t for pattern in eo_patterns):
                match = re.search(r'Executive Order (\d+)', t)
                if match:
                    eo_code = f"E.O.{match.group(1)}"
                    order_ids.append(eo_code)

            if n and isinstance(n, str):
                matches = re.findall(r'Executive Order (\d+)', n)
                for m in matches:
                    eo_code = f"E.O.{m}"
                    order_ids.append(eo_code)

        existing_order_id = row.get('order_id')
        all_order_ids = [existing_order_id] if existing_order_id else []
        all_order_ids.extend(order_ids)
        combined_order_id = "; ".join(sorted(set(filter(None, all_order_ids)))) if all_order_ids else None

        filtered_ids = [
            d for d in ids_list
            if d.get('type') is None or d.get('type').lower().strip().rstrip(':') not in remove_types_lower
        ]

        extracted_values['order_id_extracted'] = combined_order_id
        extracted_values['ids_extracted'] = filtered_ids
        return pd.Series(extracted_values)

    temp_df = temp_df.join(temp_df.apply(process_ids, axis=1))
    temp_df = temp_df.drop(columns=['ids','order_id'])

    # Add fixed columns
    temp_df['source_description'] = 'United States Department of the Treasury'
    temp_df['sanction_authority'] = 'U.S. Department of Treasury - Office of Foreign Assets Control'
    temp_df['sanction_list_name'] = 'Specially Designated Nationals (SDN) - Treasury Department'
    temp_df['sanction_authority_ids'] = '19'
    temp_df['sanction_authority_country'] = 'USA'
    temp_df['subject_country'] = 'United States'
    temp_df['source_type'] = 'Sanction'
    temp_df['is_pep'] = 'False'
    temp_df['is_sanction'] = 'True'
    temp_df['is_watchlist'] = 'False'
    temp_df['is_enforcement'] = 'False'
    temp_df['is_apc'] = 'False'

    progress_bar.progress(70)
    status_text.text("Preparing data for final transformation and explode...")

    columns_to_process = ['name', 'alt_names',
        'places_of_birth', 'addresses', 'source', 'programs', 'remarks',
        'vessel_type', 'vessel_flag', 'vessel_owner', 'citizenships',
        'dates_of_birth', 'country', 'nationalities', 'sanction_date',
        'Phone Number_extracted', 'Target Type_extracted',
        'Website_extracted', 'Organization Established Date_extracted',
        'Organization Type:_extracted', 'Email Address_extracted',
        'Gender_extracted', 'order_id_extracted', 'ids_extracted',
        'source_description', 'sanction_authority', 'sanction_list_name',
        'sanction_authority_ids', 'sanction_authority_country',
        'subject_country', 'source_type', 'is_pep', 'is_sanction',
        'is_watchlist', 'is_enforcement', 'is_apc'
    ]

    for col in columns_to_process:
        temp_df[col] = temp_df[col].apply(
            lambda x: x if isinstance(x, list) else [str(x)] if pd.notnull(x) else []
        )

    max_len = temp_df[columns_to_process].applymap(len).max(axis=1)

    for col in columns_to_process:
        temp_df[col] = temp_df.apply(lambda row: row[col] + [None] * (max_len[row.name] - len(row[col])), axis=1)

    temp_df_expanded = temp_df.explode(columns_to_process, ignore_index=True)

    temp_df_expanded.rename(columns={'country': 'country1', 'type': 'type1'}, inplace=True)

    temp_df_expanded1 = temp_df_expanded.join(temp_df_expanded['addresses'].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series({})))
    temp_df_expanded1 = temp_df_expanded1.rename(columns={'country': 'address_country'})

    temp_df_expanded1 = temp_df_expanded1.join(temp_df_expanded['ids_extracted'].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series({})))
    temp_df_expanded2 = temp_df_expanded1.rename(columns={
        'country': 'identifier_country',
        'address': 'address_street',
        'city':'address_city',
        'state':'address_province',
        'postal_code':'address_postal_code',
        'type':'identifier_name',
        'number':'identifier_value',
        'issue_date':'identifier_issue_date_date',
        'expiration_date':'identifier_expiry_date_date',
        'citizenships':'citizenship_country',
        'nationalities':'nationality_country',
        'alt_names':'alias_name',
        'type1':'type',
        'programs':'sanction_programme_name',
        'remarks':'position',
        'vessel_type':'vessel_type',
        'vessel_flag':'current_country_flag',
        'sanction_date':'sanction_action_date_date',
        'Phone Number_extracted':'contact_number',
        'Website_extracted':'website',
        'Organization Established Date_extracted':'date_of_incorporation_date',
        'Organization Type:_extracted':'sanction_remarks',
        'Email Address_extracted':'email_id',
        'Gender_extracted':'gender',
        'order_id_extracted':'sanction_order_number',
        'entity_number':'ID',
    })

    temp_df_expanded2['type'] = temp_df_expanded2['type'].replace('Entity','Organization')

    def split_pob_with_type(value):
        if pd.isna(value):
            return pd.Series([pd.NA, pd.NA, pd.NA])
        address_type = 'Birth'
        if ',' in value:
            parts = value.rsplit(',', 1)
            return pd.Series([address_type, parts[0].strip(), parts[1].strip()])
        else:
            return pd.Series([address_type, pd.NA, value.strip()])

    temp_df_expanded2[['pob_address_type', 'pob_address', 'pob_country']] = temp_df_expanded2['places_of_birth'].apply(split_pob_with_type)
    temp_df_expanded2['address_type']= np.nan

    code_to_name = dict(zip(country_code_df['countrycode2'], country_code_df['Country']))

    columns_to_map = ['address_country', 'nationality_country', 'citizenship_country','identifier_country']
    for col in columns_to_map:
        temp_df_expanded2[col] = temp_df_expanded2[col].map(code_to_name).fillna(temp_df_expanded2[col])

    code_to_countrycode3 = dict(zip(country_code_df['Country'], country_code_df['countrycode3']))

    columns_to_map2 = ['address_country', 'nationality_country', 'citizenship_country','identifier_country','pob_country']
    for col in columns_to_map2:
        temp_df_expanded2[f'{col}_code'] = temp_df_expanded2[col].map(code_to_countrycode3).fillna(temp_df_expanded2[col])

    temp_df_expanded2['sanction_action_date_month'] = temp_df_expanded2['sanction_action_date_date'].str[4:6]
    temp_df_expanded2['sanction_action_date_year'] = temp_df_expanded2['sanction_action_date_date'].str[:4]
    temp_df_expanded2['sanction_action_date_date'] = temp_df_expanded2['sanction_action_date_date'].str[6:8]

    progress_bar.progress(85)
    status_text.text("Final data adjustments and sorting...")

    temp_df_expanded3 = temp_df_expanded2[['ID-NAME', 'ID', 'type', 'name', 'alias_name',
                                           'gender','dates_of_birth','citizenship_country','citizenship_country_code',
                                           'nationality_country','nationality_country_code','date_of_incorporation_date',
                                           'vessel_type','current_country_flag','address_type',
                                           'address_street','address_city', 'address_province', 'address_postal_code',
                                           'address_country','address_country_code','contact_number','email_id',
                                           'website','identifier_name','identifier_value','identifier_country',
                                           'identifier_country_code','identifier_issue_date_date','identifier_expiry_date_date',
                                           'position','sanction_authority','sanction_authority_ids','sanction_authority_country',
                                           'sanction_list_name','sanction_action_date_date','sanction_action_date_month',
                                           'sanction_action_date_year','sanction_order_number',
                                           'sanction_programme_name','source_type','source','source_description',
                                           'is_pep', 'is_sanction', 'is_watchlist', 'is_enforcement', 'is_apc',
                                           'subject_country'
                                            ]]

    pob_df = temp_df_expanded2[['ID-NAME','ID','type','pob_address_type', 'pob_address','pob_country', 'pob_country_code']]
    pob_df = pob_df[pob_df['pob_country'].notna()]
    pob_df = pob_df.rename(columns={
        'pob_address_type': 'address_type',
        'pob_address': 'address_street',
        'pob_country': 'address_country',
        'pob_country_code': 'address_country_code'
    })

    temp_df_expanded4 = pd.concat([temp_df_expanded3, pob_df], ignore_index=True)
    temp_df_expanded4 = temp_df_expanded4.sort_values(by=['ID', 'name'],na_position='last').reset_index(drop=True)

    sort_df = temp_df_expanded4[['ID-NAME','ID','type','address_type','address_street','address_city','address_province','address_postal_code','address_country','address_country_code']]
    df_sorted = sort_df.sort_values(by=['ID', 'address_country'], na_position='last').reset_index(drop=True)
    df_sorted = df_sorted[['address_type','address_street','address_city','address_province','address_postal_code','address_country','address_country_code']]

    temp_df_expanded4 = temp_df_expanded4.drop(df_sorted.columns, axis=1)

    concatenated_df = pd.concat([temp_df_expanded4, df_sorted], axis=1)
    concatenated_df = concatenated_df.rename(columns={'name': 'entity_name','dates_of_birth':'date_of_birth_year'})

    df_org = concatenated_df[concatenated_df['type'] == 'Organization']
    df_ind = concatenated_df[concatenated_df['type'] == 'Individual']

    df_org_final = pd.concat([df_format, df_org], ignore_index=True, sort=False)
    df_ind_final = pd.concat([df_format, df_ind], ignore_index=True, sort=False)

    progress_bar.progress(95)
    status_text.text("Finalizing and preparing download...")

    st.subheader("Organization Preview")
    st.dataframe(df_org_final.head(10))

    st.subheader("Individual Preview")
    st.dataframe(df_ind_final.head(10))

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_org_final.to_excel(writer, sheet_name='organization', index=False)
        df_ind_final.to_excel(writer, sheet_name='individual', index=False)
    processed_data = output.getvalue()

    st.download_button(
        label="Download final_output.xlsx",
        data=processed_data,
        file_name="final_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    progress_bar.progress(100)
    status_text.text("Processing complete!")

else:
    st.info("Please upload df_checkv2.xlsx to proceed.")

st.info("⚠️ After downloading you have to work on ind name segregation, alias type, dob, incorporation dates, position, creation date columns")
