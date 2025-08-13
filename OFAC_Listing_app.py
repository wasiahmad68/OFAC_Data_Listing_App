import streamlit as st
import pandas as pd
import re
from io import BytesIO

# Function to extract the content of the first outer parentheses
def extract_outer_first_parentheses(text):
    stack = []
    start_index = None
    for i, char in enumerate(text):
        if char == '(':
            if start_index is None:
                start_index = i
            stack.append(i)
        elif char == ')':
            if stack:
                stack.pop()
                if not stack:
                    return text[start_index + 1:i]
    return None

# Extract name and initial alias based on conditions
def extract_name_alias(text):
    name = ''
    alias = []

    first_paren = text.find('(')
    if first_paren == -1:
        return '', []

    first_content = extract_outer_first_parentheses(text[first_paren:])
    if not first_content:
        return '', []

    # Condition 1
    if ':' in first_content and 'Linked To:' not in first_content:
        name = text[:first_paren].strip()
        alias.append(first_content.strip())
        
        remaining_text = text[first_paren + len(first_content) + 2:]
        next_paren_content = extract_outer_first_parentheses(remaining_text)
        if next_paren_content and ('a.k.a' in next_paren_content or 'f.k.a' in next_paren_content):
            for part in next_paren_content.split(';'):
                alias.append(part.strip())
        return name, alias

    # Condition 2
    elif ':' not in first_content and 'Linked To:' not in first_content:
        if 'a.k.a' in first_content or 'f.k.a' in first_content:
            name = text[:first_paren].strip()
            for part in first_content.split(';'):
                alias.append(part.strip())
            return name, alias
        else:
            return '', []

    # Condition 3
    else:
        return '', []

# Clean alias: remove quotes, extract nested parentheses, split by ';'
def clean_nested_parentheses_from_alias(alias):
    final_alias = []
    for a in alias:
        nested = re.findall(r'\((.*?)\)', a)
        cleaned_alias = re.sub(r'\(.*?\)', '', a).strip()
        cleaned_alias = cleaned_alias.replace('"', '')
        if cleaned_alias:
            for part in cleaned_alias.split(';'):
                part_clean = part.strip()
                if part_clean:
                    final_alias.append(part_clean)
        for item in nested:
            item_clean = item.strip().replace('"', '')
            if item_clean:
                final_alias.append(item_clean)
    return final_alias

# Final function to extract name and alias joined by ';'
def extract_final_name_alias_joined(text):
    name, alias = extract_name_alias(text)
    if alias:
        alias = clean_nested_parentheses_from_alias(alias)
        alias = ';'.join(alias)
    else:
        alias = ''
    return name, alias

# Streamlit app
st.title("OFAC List Name and Alias Extractor")
st.markdown("**Please upload your OFAC Excel file (`raw_ofac_list.xlsx`)**")

uploaded_file = st.file_uploader("Upload `raw_ofac_list.xlsx`", type=['xlsx'])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    column_name = st.selectbox("Select the column containing the list data", df.columns)

    st.info("Processing data, please wait...")
    df[['name', 'alias']] = df[column_name].apply(lambda x: pd.Series(extract_final_name_alias_joined(x)))
    st.success("Data processed successfully!")

    st.write("### Extracted Data")
    st.dataframe(df[[column_name, 'name', 'alias']])

    # Convert DataFrame to Excel in-memory
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
