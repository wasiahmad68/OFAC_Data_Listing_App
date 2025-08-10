import json
import pandas as pd
import numpy as np
import re

# importing df_checkv2.xlsx
df_checkv2 = pd.read_excel('df_checkv2.xlsx', dtype={'id': 'Int64'})
df_checkv2.shape
# importing countrycode df
country_code_df = pd.read_excel(r"Country_code.xlsx",
                   keep_default_na=False, na_values=[])
# importing format of ofac listing
df_format = pd.read_excel('OFAC_format.xlsx')


# combining id and name column
df_checkv2["id-name"] = df_checkv2["id"].astype("string").fillna('') + "-" + df_checkv2["name"].fillna('').astype(str)
df_checkv2.head()
# Importing the json ofac data
import requests

url = "https://data.trade.gov/downloadable_consolidated_screening_list/v1/consolidated.json"  # clean URL without query tags
resp = requests.get(url)
resp.raise_for_status()

data = resp.json()

# Again, adjust 'entries' to match the JSON structure you're working with
df_json = pd.json_normalize(data,"results")

df_json.head()
df_json.shape
# combining id and name column
df_json["ID-NAME"] = df_json["id"] + "-" + df_json["name"]

# filtering checking df_check in df_json
filtered_df = df_json[df_json['ID-NAME'].isin(df_checkv2['id-name'])]



# Create a mapping dictionary from df_check
name_to_source = dict(zip(df_checkv2['id-name'], df_checkv2['source']))
idname_to_alias = dict(zip(df_checkv2['id-name'], df_checkv2['alias']))


# Add the 'Source' column to filtered_df2 using map
filtered_df['source'] = filtered_df['ID-NAME'].map(name_to_source)
filtered_df['alias2'] = filtered_df['ID-NAME'].map(idname_to_alias)
filtered_df.head()
filtered_df.shape
# droping duplicate cases based on same id-names
filtered_df2 = filtered_df.drop_duplicates(subset=['ID-NAME'], keep='last')
filtered_df2.shape

# Convert alias2 strings into lists by splitting on ';', handle NaN safely
filtered_df2['alias2'] = filtered_df['alias2'].fillna('').apply(lambda x: [item.strip() for item in x.split(';') if item.strip()])

# Combine and remove duplicates using set, convert back to list for alias2 and alt_names columns
filtered_df2['alt_names'] = filtered_df2.apply(lambda row: list(set(row['alt_names']) | set(row['alias2'])), axis=1)


# Drop alias2 column not needed now
filtered_df2.drop(columns=['alias2'], inplace=True)
temp_df = filtered_df2[['ID-NAME','entity_number', 'type','name','alt_names',
                        'places_of_birth','addresses','source', 'programs',                
                      'remarks','vessel_type','vessel_flag','vessel_owner',
                      'citizenships','dates_of_birth','country','nationalities','ids']]
temp_df['remarks'] = temp_df['remarks'].fillna('').str.split(';')
temp_df = temp_df.reset_index(drop=True)
temp_df.head()

# extracting sanction date from source link
temp_df['sanction_date'] = temp_df['source'].str.rsplit('/', n=1).str[-1].str[:8]

# extracting order_id from program name if available
import re
# function to extract from program name if found
def extract_order_ids(program_list):
    pattern = r'EO\d+'
    matches = []
    for item in program_list:
        if item:
            matches.extend(re.findall(pattern, item))
    # Replace 'EO' with 'E.O.' in all matched strings
    matches = [m.replace('EO', 'E.O.') for m in matches]
    return "; ".join(matches)

temp_df['order_id'] = temp_df['programs'].apply(extract_order_ids)
# joining all program separated by ;
temp_df['programs'] = temp_df['programs'].apply(
    lambda x: '; '.join(str(i) for i in x if i is not None) if isinstance(x, list) else x
)
#convert string list to actual list
import ast

def parse_ids_column(val):
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except (ValueError, SyntaxError):
            return []
    return val

temp_df['ids'] = temp_df['ids'].apply(parse_ids_column)
# this function extract all usefull values from ids into seperate column and extract all E.O. form ids key and type key
import pandas as pd
import re

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
] + wanted_types  # Also remove extracted wanted types from ids

# Prepare a normalized list for comparison (lowercase, stripped, no trailing colon)
remove_types_lower = [t.lower().strip().rstrip(':') for t in remove_types]

def process_ids(row):
    ids_list = row.get('ids')
    if not isinstance(ids_list, list):
        # If ids_list is not a list, return None/NaN for all extracted fields
        data = {f"{k}_extracted": None for k in wanted_types}
        data['order_id_extracted'] = row.get('order_id', None)
        data['ids_extracted'] = ids_list
        return pd.Series(data)

    extracted_values = {f"{k}_extracted": None for k in wanted_types}
    order_ids = []

    for d in ids_list:
        t = d.get('type')
        n = d.get('number')

        # Extract wanted types values
        if t and t in wanted_types:
            extracted_values[f"{t}_extracted"] = n

        # Extract EO codes from type field
        if t and any(pattern in t for pattern in eo_patterns):
            match = re.search(r'Executive Order (\d+)', t)
            if match:
                eo_code = f"E.O.{match.group(1)}"
                order_ids.append(eo_code)

        # NEW: Extract EO codes from number field (string)
        if n and isinstance(n, str):
            matches = re.findall(r'Executive Order (\d+)', n)
            for m in matches:
                eo_code = f"E.O.{m}"
                order_ids.append(eo_code)

    # Combine existing order_id with extracted EO codes, remove duplicates
    existing_order_id = row.get('order_id')
    all_order_ids = [existing_order_id] if existing_order_id else []
    all_order_ids.extend(order_ids)
    # Remove duplicates and join with "; "
    combined_order_id = "; ".join(sorted(set(filter(None, all_order_ids)))) if all_order_ids else None

    # Filter out unwanted 'type's robustly (ignore case, strip spaces and trailing colon)
    filtered_ids = [
        d for d in ids_list
        if d.get('type') is None or d.get('type').lower().strip().rstrip(':') not in remove_types_lower
    ]

    extracted_values['order_id_extracted'] = combined_order_id
    extracted_values['ids_extracted'] = filtered_ids
    return pd.Series(extracted_values)

# Usage example:

temp_df = temp_df.join(temp_df.apply(process_ids, axis=1))
temp_df = temp_df.drop(columns=['ids','order_id'])
# add fixed columns  
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
# Ensure all columns have lists and replace NaN or other non-list types with empty lists

columns_to_process = ['name', 'alt_names',
       'places_of_birth', 'addresses', 'source', 'programs', 'remarks',
       'vessel_type', 'vessel_flag', 'vessel_owner', 'citizenships',
       'dates_of_birth', 'country', 'nationalities', 'sanction_date'
       , 'Phone Number_extracted', 'Target Type_extracted',
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

# Step 1: Get the max length of lists across the columns
max_len = temp_df[columns_to_process].applymap(len).max(axis=1)

# Step 2: Pad the shorter lists with None
for col in columns_to_process:
    temp_df[col] = temp_df.apply(lambda row: row[col] + [None] * (max_len[row.name] - len(row[col])), axis=1)

# Step 3: Explode the lists into separate rows
temp_df_expanded = temp_df.explode(columns_to_process, ignore_index=True)
temp_df_expanded.shape

# Renaming country and type column to solve the naming confilict with country and type in address dictionary expanding into new columns  
temp_df_expanded.rename(columns={'country': 'country1', 'type': 'type1'}, inplace=True)

# Ensure all columns have lists and replace NaN or other non-list types with empty lists

columns_to_process = ['name', 'alt_names',
       'places_of_birth', 'addresses', 'source', 'programs', 'remarks',
       'vessel_type', 'vessel_flag', 'vessel_owner', 'citizenships',
       'dates_of_birth', 'country', 'nationalities', 'sanction_date'
       , 'Phone Number_extracted', 'Target Type_extracted',
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

# Step 1: Get the max length of lists across the columns
max_len = temp_df[columns_to_process].applymap(len).max(axis=1)

# Step 2: Pad the shorter lists with None
for col in columns_to_process:
    temp_df[col] = temp_df.apply(lambda row: row[col] + [None] * (max_len[row.name] - len(row[col])), axis=1)

# Step 3: Explode the lists into separate rows
temp_df_expanded = temp_df.explode(columns_to_process, ignore_index=True)

# Display the expanded DataFrame
temp_df_expanded
# Renaming country and type column to solve the naming confilict with country and type in address dictionary expanding into new columns  
temp_df_expanded.rename(columns={'country': 'country1', 'type': 'type1'}, inplace=True)

# Handle None values and apply pd.Series to expand the dictionary into new seperate columns here "addresses" and "ids" are dictionary columns
temp_df_expanded1 = temp_df_expanded.join(temp_df_expanded['addresses'].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series({})))
# Renaming the country name which we get from expanding address dictionary to solve the naming confilict with country in ids dictionary 
temp_df_expanded1 = temp_df_expanded1.rename(columns={'country': 'address_country'})
# expanding ids column dictionary into separate columns 
temp_df_expanded1 = temp_df_expanded1.join(temp_df_expanded['ids_extracted'].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series({})))
temp_df_expanded2 = temp_df_expanded1.rename(columns={'country': 'identifier_country',
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

# spiliting birth address into different columns
import numpy as np
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


# Create a mapping dictionary from country_code_df
code_to_name = dict(zip(country_code_df['countrycode2'], country_code_df['Country']))

# Replace country codes with country names in the specified columns of temp_df_expanded2
columns_to_map = ['address_country', 'nationality_country', 'citizenship_country','identifier_country']
for col in columns_to_map:
    temp_df_expanded2[col] = temp_df_expanded2[col].map(code_to_name).fillna(temp_df_expanded2[col])

# Create mapping dictionaries for countrycode3
code_to_countrycode3 = dict(zip(country_code_df['Country'], country_code_df['countrycode3']))

# Add new columns for 'countrycode3' based on 'country', 'nationality','pob_country' and 'citizen'
columns_to_map2 = ['address_country', 'nationality_country', 'citizenship_country','identifier_country','pob_country']
for col in columns_to_map2:
    temp_df_expanded2[f'{col}_code'] = temp_df_expanded2[col].map(code_to_countrycode3).fillna(temp_df_expanded2[col])

# spliting the sanction date
temp_df_expanded2['sanction_action_date_month'] = temp_df_expanded2['sanction_action_date_date'].str[4:6]       # 5th and 6th characters
temp_df_expanded2['sanction_action_date_year'] = temp_df_expanded2['sanction_action_date_date'].str[:4]     # Left 4 characters
temp_df_expanded2['sanction_action_date_date'] = temp_df_expanded2['sanction_action_date_date'].str[6:8]       # 7th and 8th characters

# selecting important columns except pob address and rearranging them in order
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
# selecting seperate pob address columns and not nan values
pob_df = temp_df_expanded2[['ID-NAME','ID','type','pob_address_type', 'pob_address','pob_country', 'pob_country_code']]
pob_df = pob_df[pob_df['pob_country'].notna()]
pob_df = pob_df.rename(columns={
    'pob_address_type': 'address_type',
    'pob_address': 'address_street',
    'pob_country': 'address_country',
    'pob_country_code': 'address_country_code'
})
# adding pob_df value below temp_df_expanded3
temp_df_expanded4 = pd.concat([temp_df_expanded3, pob_df], ignore_index=True)
# sort based on ID column and 
temp_df_expanded4 = temp_df_expanded4.sort_values(by=['ID', 'name'],na_position='last').reset_index(drop=True)
# extracting the column whic need to be sorted from main temp_df_expanded4
sort_df = temp_df_expanded4[['ID-NAME','ID','type','address_type','address_street','address_city','address_province','address_postal_code','address_country','address_country_code']]
# Sort by entity_number ascending and address_country with NaNs last
df_sorted = sort_df.sort_values(
    by=['ID', 'address_country'],
    na_position='last'
).reset_index(drop=True)
df_sorted = df_sorted[['address_type','address_street','address_city','address_province','address_postal_code','address_country','address_country_code']]
# removing unsorted address columns from temp_df_expanded4
temp_df_expanded4 = temp_df_expanded4.drop(df_sorted.columns, axis=1)
# final step adding sorted address including pob into temp_df_expanded4 as we have sorted both temp_df_expanded4 and df_sorted so it will aligned
concatenated_df = pd.concat([temp_df_expanded4, df_sorted], axis=1)
concatenated_df = concatenated_df.rename(columns={'name': 'entity_name','dates_of_birth':'date_of_birth_year'})
concatenated_df.head()
# Filter concatenated_df based on 'type'
df_org = concatenated_df[concatenated_df['type'] == 'Organization']
df_ind = concatenated_df[concatenated_df['type'] == 'Individual']

# Append to df_format to align columns and add missing columns as NaN
df_org_final = pd.concat([df_format, df_org], ignore_index=True, sort=False)
df_ind_final = pd.concat([df_format, df_ind], ignore_index=True, sort=False)

# Export to Excel with two sheets
with pd.ExcelWriter('final_output.xlsx') as writer:
    df_org_final.to_excel(writer, sheet_name='organization', index=False)
    df_ind_final.to_excel(writer, sheet_name='individual', index=False)
