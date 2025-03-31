-- Creating patient_data external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.dev_healthcare_data.patient_data_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://health-data-bucket-ju/dev/patient_data.csv'],
  skip_leading_rows = 1
);

-- Creating ehr_data external table (JSON format)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.dev_healthcare_data.ehr_data_external`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://health-data-bucket-ju/dev/ehr_data.json']
);

-- Creating claims_data external table (Parquet format with explicit schema)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.dev_healthcare_data.claims_data_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://health-data-bucket-ju/dev/claims_data.parquet']
);


----------------

-- Creating patient_data external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.prod_healthcare_data.patient_data_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://health-data-bucket-ju/prod/patient_data.csv'],
  skip_leading_rows = 1
);

-- Creating ehr_data external table (JSON format)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.prod_healthcare_data.ehr_data_external`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://health-data-bucket-ju/prod/ehr_data.json']
);

-- Creating claims_data external table (Parquet format with explicit schema)
CREATE OR REPLACE EXTERNAL TABLE `dbt-health-data-project.prod_healthcare_data.claims_data_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://health-data-bucket-ju/prod/claims_data.parquet']
);
