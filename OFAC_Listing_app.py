import streamlit as st
import pandas as pd
import re
from io import BytesIO

# Function to extract name and alias
def extract_name_alias(text):
    if not isinstance(text, str) or not text.strip():
        return pd.Series({'name': '', 'alias': ''})

    all_brackets = re.findall(r'\(([^()]*)\)', text)
    if not all_brackets:
        return pd.Series({'name': '', 'alias': ''})

    first_bracket = all_brackets[0]

    if 'Linked To:' in first_bracket:
        return pd.Series({'name': '', 'alias': ''})

    if ('a.k.a.' not in first_bracket and 'f.k.a.' not in first_bracket) and (':' not in first_bracket):
        return pd.Series({'name': '', 'alias': ''})

    name = text.split('(', 1)[0].strip()

    alias_list = []
    if 'a.k.a.' not in first_bracket and 'f.k.a.' not in first_bracket:
        alias_list.append(first_bracket.strip())

    aka_matches = re.findall(r'\(([^()]*(?:a\.k\.a\.|f\.k\.a\.).*?)\)', text)
    for match in aka_matches:
        parts = re.split(r'a\.k\.a\.|f\.k\.a\.', match)
        for part in parts[1:]:
            aliases = [p.strip() for p in part.split(';') if p.strip()]
            for alias in aliases:
                nested = re.findall(r'\(([^()]*)\)', alias)
                if nested:
                    alias_main = re.sub(r'\([^()]*\)', '', alias).strip()
                    alias_list.append(alias_main)
                    for n in nested:
                        alias_list.append(n.strip())
                else:
                    alias_list.append(alias)

    seen = set()
    final_aliases = []
    for a in alias_list:
        if a not in seen:
            seen.add(a)
            final_aliases.append(a)

    return pd.Series({'name': name, 'alias': '; '.join(final_aliases)})

st.title("OFAC List Name and Alias Extractor")

st.markdown("**Please upload the file named `raw_ofac_list.xlsx`**")

uploaded_file = st.file_uploader("Upload `raw_ofac_list.xlsx`", type=['xlsx'])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    column_name = st.selectbox("Select the column containing the list data", df.columns)

    df[['name', 'alias']] = df[column_name].apply(extract_name_alias)

    st.write("### Extracted Data")
    st.dataframe(df[[column_name, 'name', 'alias']])

    # Convert DataFrame to Excel in-memory using openpyxl
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()

    st.download_button(
        label="Download df_check.xlsx",
        data=processed_data,
        file_name='df_check.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
st.info("⚠️ Please verify after downloading that all 'name' values are properly extracted and not empty.")