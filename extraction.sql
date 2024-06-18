USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

LS @doc_ai_stage;

ALTER WAREHOUSE doc_ai_qs_wh
SET WAREHOUSE_SIZE = XSMALL
    WAIT_FOR_COMPLETION = TRUE;

-- Create a table with all values and scores
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_BRANDING_AGREEMENTS
AS
WITH 
-- First part gets the result from applying the model on the pdf documents as a JSON with additional metadata
temp as(
    SELECT 
        Relative_path as file_name
        , size as file_size
        , last_modified
        , file_url as snowflake_file_url
        -- VERIFY THAT BELOW IS USING THE SAME NAME AND NUMER AS THE MODEL INSTRUCTIONS YOU COPIED IN THE PREVIOUS STEP!
        ,  DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_QS_CO_BRANDING!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1) as json
    from directory(@doc_ai_stage)
)
-- Second part extract the values and the scores from the JSON into columns
SELECT
file_name
, file_size
, last_modified
, snowflake_file_url
, json:__documentMetadata.ocrScore::FLOAT AS ocrScore
, json:parties::ARRAY as parties_array
, ARRAY_SIZE(parties_array) AS identified_parties
, json:effective_date[0]:score::FLOAT AS effective_date_score
, json:effective_date[0]:value::STRING AS effective_date_value
, json:duration[0]:score::FLOAT AS agreement_duration_score
, json:duration[0]:value::STRING AS agreement_duration_value
, json:notice_period[0]:score::FLOAT AS notice_period_score
, json:notice_period[0]:value::STRING AS notice_period_value
, json:payment_terms[0]:score::FLOAT AS payment_terms_score
, json:payment_terms[0]:value::STRING AS payment_terms_value
, json:force_majeure[0]:score::FLOAT AS have_force_majeure_score
, json:force_majeure[0]:value::STRING AS have_force_majeure_value
, json:indemnification_clause[0]:score::FLOAT AS have_indemnification_clause_score
, json:indemnification_clause[0]:value::STRING AS have_indemnification_clause_value
, json:renewal_options[0]:score::FLOAT AS have_renewal_options_score
, json:renewal_options[0]:value::STRING AS have_renewal_options_value
FROM temp;  

-- Check that there is a result by running the following SQL  
select * from doc_ai_qs_db.doc_ai_schema.CO_BRANDING_AGREEMENTS;
