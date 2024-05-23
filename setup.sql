USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE doc_ai_qs_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_qs_role;

GRANT ROLE doc_ai_qs_role TO USER <YOUR USER>;

-- CREATE A WAREHOUSE TO BE USED
CREATE WAREHOUSE doc_ai_qs_wh;

-- GIVE THE doc_ai_qs_role ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE doc_ai_qs_wh TO ROLE doc_ai_qs_role;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE doc_ai_qs_role ACCESS
CREATE DATABASE doc_ai_qs_db;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE doc_ai_qs_db TO ROLE doc_ai_qs_role;

-- CHANGE TO THE QUICKSTART ROLE
USE ROLE doc_ai_qs_role;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE SCHEMA doc_ai_qs_db.doc_ai_schema;
-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE snowflake.ml.document_intelligence on schema doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE STAGE doc_ai_qs_db.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

-- SCHEMA FOR THE STREAMLIT APP
CREATE SCHEMA doc_ai_qs_db.streamlit_schema;

-- TABLE FOR THE STREAMLIT APP
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_BRANDING_AGREEMENTS_VERIFIED
(
    file_name string
    , snowflake_file_url string
    , verification_date TIMESTAMP
    , verification_user string
);
