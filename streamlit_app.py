# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as snow_funcs

import pypdfium2 as pdfium
from datetime import datetime

st.set_page_config(layout="wide")

# Write directly to the app
st.title("Co-Branding Agreement Verification :ledger:")
st.write(
    """A example Streamlit Application that enables users to verify values that is missing or have a extraction score below a threshold.
    """
)

# Get the current credentials
session = get_active_session()

#
#  Parameters
# 
doc_ai_context = "doc_ai_qs_db.doc_ai_schema"
doc_ai_source_table = "CO_BRANDING_AGREEMENTS"
doc_ai_source_verify_table = "CO_BRANDING_AGREEMENTS_VERIFIED"
doc_ai_doc_stage = "doc_ai_stage"

# Dict that has the name of the columns that needs to be verified, it has the column name of the column 
# with value and column with the score
value_dict = {
    "EFFECTIVE_DATE": {
        "VAL_COL": "EFFECTIVE_DATE_VALUE",
        "SCORE_COL": "EFFECTIVE_DATE_SCORE"
    },
    "AGREEMENT_DURATION": {
        "VAL_COL": "AGREEMENT_DURATION_VALUE",
        "SCORE_COL": "AGREEMENT_DURATION_SCORE"
    },
    "NOTICE_PERIOD": {
        "VAL_COL": "NOTICE_PERIOD_VALUE",
        "SCORE_COL": "NOTICE_PERIOD_SCORE"
    },
    "PAYMENT_TERMS": {
        "VAL_COL": "PAYMENT_TERMS_VALUE",
        "SCORE_COL": "PAYMENT_TERMS_SCORE"
    },
    "HAVE_FORCE_MAJEURE": {
        "VAL_COL": "HAVE_FORCE_MAJEURE_VALUE",
        "SCORE_COL": "HAVE_FORCE_MAJEURE_SCORE"
    },
    "HAVE_INDEMNIFICATION_CLAUSE": {
        "VAL_COL": "HAVE_INDEMNIFICATION_CLAUSE_VALUE",
        "SCORE_COL": "HAVE_INDEMNIFICATION_CLAUSE_SCORE"
    },
    "HAVE_RENEWAL_OPTIONS": {
        "VAL_COL": "HAVE_RENEWAL_OPTIONS_VALUE",
        "SCORE_COL": "HAVE_RENEWAL_OPTIONS_SCORE"
    }
}

# The minimum score needed to not be verified
threshold_score = 0.5

# HELPER FUNCTIONS
# Function to generate filter to only get the rows that are missing values or have a score below the threashold
def generate_filter(col_dict:dict,  score_val:float): #score_cols:list, score_val:float, val_cols:list):
    
    filter_exp = ''

    # For each column
    for col in col_dict:
        # Create the filter on score threashold or missing value
        if len(filter_exp) > 0:
                filter_exp += ' OR '
        filter_exp += f'(({col_dict[col]["SCORE_COL"]} <= {score_val} ) OR ({col_dict[col]["VAL_COL"]} IS NULL))'

    if len(filter_exp) > 0:
       filter_exp = f'({filter_exp}) AND ' 
    
    # Filter out documents already verified
    filter_exp  += 'verification_date is null'
    return filter_exp

# Generates a column list for counting the number of documents that is missing values or a score less that the threashold
# by each column
def count_missing_select(col_dict:dict, score_val:float):
    select_list = []

    for col in col_dict:
        col_exp = (snow_funcs.sum(
                          snow_funcs.iff(
                                    (
                                        (snow_funcs.col(col_dict[col]["VAL_COL"]).is_null())
                                        | 
                                        (snow_funcs.col(col_dict[col]["SCORE_COL"]) <= score_val)
                                    ), 1,0
                              )
                      ).as_(col)
                )
        select_list.append(col_exp)
        
    return select_list

# Function to display a pdf page
def display_pdf_page():
    pdf = st.session_state['pdf_doc']
    page = pdf[st.session_state['pdf_page']]
            
    bitmap = page.render(
                    scale = 8, 
                    rotation = 0,
            )
    pil_image = bitmap.to_pil()
    st.image(pil_image)

# Function to move to the next PDF page
def next_pdf_page():
    if st.session_state.pdf_page + 1 >= len(st.session_state['pdf_doc']):
        st.session_state.pdf_page = 0
    else:
        st.session_state.pdf_page += 1

# Function to move to the previous PDF page
def previous_pdf_page():
    if st.session_state.pdf_page > 0:
        st.session_state.pdf_page -= 1

# Function to get the name of all documents that need verification
def get_documents(doc_df):
    
    lst_docs = [dbRow[0] for dbRow in doc_df.collect()]
    # Add a default None value
    lst_docs.insert(0, None)
    return lst_docs

# MAIN

# Get the table with all documents with extracted values
df_agreements = session.table(f"{doc_ai_context}.{doc_ai_source_table}")

# Get the documents we already gave verified
df_validated_docs = session.table(f"{doc_ai_context}.{doc_ai_source_verify_table}")

# Join
df_all_docs = df_agreements.join(df_validated_docs,on='file_name', how='left', lsuffix = '_L', rsuffix = '_R')

# Filter out all document that has missing values of score below the threasholds
validate_filter = generate_filter(value_dict, threshold_score)
df_validate_docs = df_all_docs.filter(validate_filter)
col1, col2 = st.columns(2)
col1.metric(label="Total Documents", value=df_agreements.count())
col2.metric(label="Documents Needing Validation", value=df_validate_docs.count())

# Get the number of documents by value that needs verifying
select_list = count_missing_select(value_dict, threshold_score)
df_verify_counts = df_validate_docs.select(select_list)
verify_cols = df_verify_counts.columns

st.subheader("Number of documents needing validation by extraction value")
st.bar_chart(data=df_verify_counts.unpivot("needs_verify", "check_col", verify_cols), x="CHECK_COL", y="NEEDS_VERIFY")

# Verification section
st.subheader("Documents to review")
with st.container():
    # Get the name of the documents that needs verifying and add them to a listbox
    lst_documents = get_documents(df_validate_docs)
    sel_doc = st.selectbox("Document", lst_documents)

    # If we havse selected a document
    if sel_doc:        
        # Display the extracted values
        df_doc = df_validate_docs.filter(snow_funcs.col("FILE_NAME") == sel_doc)
        col_val, col_doc = st.columns(2)
        with col_val:
            with st.form("doc_form"):
                approve_checkboxes = 0
                for col in value_dict:
                    st.markdown(f"**{col}**:")
                    col_vals = df_doc[[value_dict[col]["SCORE_COL"], value_dict[col]["VAL_COL"]]].collect()[0]
                    # If we are missing a value
                    if not col_vals[1]:
                        st.markdown(f":red[**Value missing!**]")
                        st.checkbox("Approved", key=f"check_{approve_checkboxes}")
                        approve_checkboxes += 1
                    else:
                        # If the extraction is less that the threashold
                        if col_vals[0] <= threshold_score:
                            st.markdown(f":red[{col_vals[1]}]")
                            st.markdown(f":red[**The value score, {col_vals[0]}, is below threshold score!**]")
                            st.checkbox("Approved", key=f"check_{approve_checkboxes}")
                            approve_checkboxes += 1
                        else:
                            st.write(col_vals[1])
                save = st.form_submit_button()
                if save:
                     with st.spinner("Saving document approval..."):
                        for i in range(approve_checkboxes):
                            if not st.session_state[f"check_{i}"]:
                                st.error("Need to apporve all checks before saving")
                                st.stop()
                        # Create a SQL to save that the document is verified
                        insert_sql = f"INSERT INTO {doc_ai_context}.{doc_ai_source_verify_table} (file_name, verification_date) VALUES ('{sel_doc}', '{datetime.now().isoformat()}')"
                        _ = session.sql(insert_sql).collect()
                        st.success("✅ Success!")
                        # Rerun is used to force the application to run from the begining so we can not verify the same document twice
                        st.experimental_rerun()
            # Display of PDF section
            with col_doc:
                if 'pdf_page' not in st.session_state:
                    st.session_state['pdf_page'] = 0
    
                if 'pdf_url' not in st.session_state:
                    st.session_state['pdf_url'] = sel_doc
                
                if 'pdf_doc' not in st.session_state or st.session_state['pdf_url'] != sel_doc:
                    pdf_stream = session.file.get_stream(f"@{doc_ai_context}.{doc_ai_doc_stage}/{sel_doc}")
                    pdf = pdfium.PdfDocument(pdf_stream)
                    st.session_state['pdf_doc'] = pdf
                    st.session_state['pdf_url'] = sel_doc
                    st.session_state['pdf_page'] = 0
                
                nav_col1, nav_col2, nav_col3 = st.columns(3)
                with nav_col1:
                    if st.button("⏮️ Previous", on_click=previous_pdf_page):
                        pass    
                with nav_col2:
                    st.write(f"page {st.session_state['pdf_page'] +1} of {len(st.session_state['pdf_doc'])} pages")
                with nav_col3:
                    if st.button("Next ⏭️", on_click=next_pdf_page):
                        pass
                        
                display_pdf_page()
