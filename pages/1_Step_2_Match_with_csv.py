import streamlit as st
import pandas as pd
from io import BytesIO

# Set the page title for browser tab (page bar)
st.set_page_config(page_title="Match name with csv")

st.title("OFAC Name Matching Tool With CSV Online Data")

# Upload df_check.xlsx
uploaded_df_check = st.file_uploader("**Upload df_check.xlsx**", type=["xlsx"])

if uploaded_df_check:
    df_check = pd.read_excel(uploaded_df_check)
    st.write("### Uploaded df_check.xlsx Preview")
    st.dataframe(df_check.head())

    # Load OFAC CSV from URL
    url = "https://data.trade.gov/downloadable_consolidated_screening_list/v1/consolidated.csv"
    st.write("Loading OFAC consolidated list from:", url)
    df_csv = pd.read_csv(url)
    st.success(f"OFAC list loaded with {df_csv.shape[0]} rows and {df_csv.shape[1]} columns.")

    st.write("### OFAC Data Preview")
    st.dataframe(df_csv.head())

    # Matching function (your original logic)
    def match_names(df_check, df_csv):
        name_matched_list = []
        id_list = []

        for name in df_check["name"]:
            matches = df_csv[df_csv["name"] == name]

            if len(matches) == 1:
                name_matched_list.append(matches.iloc[0]["name"])
                id_list.append(matches.iloc[0]["_id"])
            elif len(matches) > 1:
                name_matched_list.append("multiple_matches_need_to_check")
                id_list.append(None)
            else:
                name_matched_list.append(None)
                id_list.append(None)

        df_check["name_matched"] = name_matched_list
        df_check["id"] = id_list
        return df_check

    # Apply matching
    st.write("### Matching names...")
    df_check = match_names(df_check, df_csv)

    # Show results
    st.write("### Matching Results")
    st.dataframe(df_check)

    # Convert DataFrame to Excel in-memory for df_check
    output_check = BytesIO()
    with pd.ExcelWriter(output_check, engine='openpyxl') as writer:
        df_check.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data_check = output_check.getvalue()

    st.download_button(
        label="Download df_checkv2.xlsx",
        data=processed_data_check,
        file_name="df_checkv2.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Convert df_csv to Excel in-memory for download
    output_csv = BytesIO()
    with pd.ExcelWriter(output_csv, engine='openpyxl') as writer:
        df_csv.to_excel(writer, index=False, sheet_name='OFAC_Data')
    processed_data_csv = output_csv.getvalue()

    st.download_button(
        label="Download OFAC consolidated CSV as Excel",
        data=processed_data_csv,
        file_name="OFAC_consolidated.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.info("⚠️ Please verify after downloading that all 'name_matched' values are properly matched and ids are unique and no multiple matches.")
