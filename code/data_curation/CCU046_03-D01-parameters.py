# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU046_03-D01-parameters
# MAGIC
# MAGIC **Project** CCU046_03
# MAGIC
# MAGIC **Description** This notebook defines a set of parameters, which is loaded in each notebook in the data curation pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Author(s)** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU003_05-D01-parameters and CCU002_07
# MAGIC
# MAGIC **Notes** This pipeline has an initial production date of 2023-03-30 (`pipeline_production_date` == `2023-03-30`) and the `archived_on` dates used for each dataset correspond to the latest (most recent) batch of data before this date. Should the pipeline and all the notebooks that follow need to be updated and rerun, then this notebook should be rerun directly (before being called by subsequent notebooks) with `pipeline_production_date` updated and `run_all_toggle` switched to True. After this notebook has been rerun the `run_all_toggle` should be reset to False to prevent subsequent notebooks that call this notebook from having to rerun the 'archived_on' section. Rerunning this notebook with the updated `pipeline_production_date` will ensure that the `archived_on` dates used for each dataset are updated with these dates being saved for reference in the collabortion database.
# MAGIC
# MAGIC **Versions** Version 1 as at '2023-03-30'
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`ccu007_05_parameters_df_datasets`**: table of `archived_on` dates for each dataset that can be used consistently throughout the pipeline 

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

# COMMAND ----------

run_all_toggle = False

# COMMAND ----------

spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Libraries

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Common Functions

# COMMAND ----------

# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Custom Functions

# COMMAND ----------

# DBTITLE 1,save_table
def save_table(df, out_name:str, save_previous=True, data_base:str=f'dsa_391419_j3w9t_collab'):
  
  # assert that df is a dataframe
  assert isinstance(df, f.DataFrame), 'df must be of type dataframe' #isinstance(df, pd.DataFrame) | 
  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))
  
  # save name check
  if(any(char.isupper() for char in out_name)): 
    print(f'Warning: {out_name} converted to lowercase for saving')
    out_name = out_name.lower()
    print('out_name: ' + out_name)
    print('')
  
  # df name
  df_name = [x for x in globals() if globals()[x] is df][0]
  
  # ------------------------------------------------------------------------------------------------
  # save previous version for comparison purposes
  if(save_previous):
    tmpt = (
      spark.sql(f"""SHOW TABLES FROM {data_base}""")
      .select('tableName')
      .where(f.col('tableName') == out_name)
      .collect()
    )
    if(len(tmpt)>0):
      # save with production date appended
      _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
      out_name_pre = f'{out_name}_pre{_datetimenow}'.lower()
      print(f'saving (previous version):')
      print(f'  {out_name}')
      print(f'  as')
      print(f'  {out_name_pre}')
      spark.table(f'{data_base}.{out_name}').write.mode('overwrite').saveAsTable(f'{data_base}.{out_name_pre}')
      #spark.sql(f'ALTER TABLE {data_base}.{out_name_pre} OWNER TO {data_base}')
      print('saved')
      print('') 
    else:
      print(f'Warning: no previous version of {out_name} found')
      print('')
  # ------------------------------------------------------------------------------------------------  
  
  # save new version
  print(f'saving:')
  print(f'  {df_name}')
  print(f'  as')
  print(f'  {out_name}')
  df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(f'{data_base}.{out_name}')
  #spark.sql(f'ALTER TABLE {data_base}.{out_name} OWNER TO {data_base}')
  print('saved')

# COMMAND ----------

# DBTITLE 1,temp_save
# temp_save lives in SHDS common functions but since migration _dbc has not been updated to new collab db dsa - will use this temp_save function here temporarily

def temp_save(df, out_name:str, _dbc:str=f'dsa_391419_j3w9t_collab'):
  
  spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
  
  dfname = [x for x in globals() if globals()[x] is df]
  if(len(dfname) == 1): dfname = dfname[0]
  else: dfname = "unknown dfname (not in globals())"      
  
  print(f'saving {dfname} to {_dbc}.{out_name}')
  if(out_name != out_name.lower()):
    out_name_old = out_name
    out_name == out_name.lower()
    print(f'  out_name changed to lower case (from {out_name_old} to {out_name})')
    
  # save  
  df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{_dbc}.{out_name}')
  #spark.sql(f'ALTER TABLE {_dbc}.{out_name} OWNER TO {_dbc}')
  print(f'  saved')
  
  # repoint
  spark.sql(f'REFRESH TABLE {_dbc}.{out_name}')
  return spark.table(f'{_dbc}.{out_name}')

# COMMAND ----------

# DBTITLE 1,extract_batch_from_archive
# Updated function that compares the number of rows expected (the number that were found when running the parameters notebook in full) against the number of rows observed (the number that were found when extracting the data from the archive in a subsequent notebook). This would alert us to the number of rows being changed in the archive tables, which the data wranglers control.

# function to extract the batch corresponding to the pre-defined archived_on date - will be used in subsequent notebooks

from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']
  _n_rows_expected = _row['n']  
  print(_path + ' (archived_on = ' + _archived_on + ', n_rows_expected = ' + _n_rows_expected + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _n_rows_observed = _tmp.count()
  print(f'  n_rows_observed = {_n_rows_observed:,}')
  assert _n_rows_observed > 0, f"_n_rows_observed == 0"
  assert f'{_n_rows_observed:,}' == _n_rows_expected, f"_n_rows_observed != _n_rows_expected ({_n_rows_observed:,} != {_n_rows_expected})"

  # return dataframe
  return _tmp

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Paths and Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Set Project Specific Variables

# COMMAND ----------

# Please set and check the variables below

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'{db}_collab'
dsa = f'dsa_391419_j3w9t_collab'
dss = 'dss_corporate'

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu046_03'

# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
study_start_date_primary = '2019-11-01' 
study_start_date_secondary = '2007-04-01'
study_end_date = '2023-12-31'

# -----------------------------------------------------------------------------
# Pipeline production date
# -----------------------------------------------------------------------------
# date at which pipeline was created and archived_on dates for datasets have been selected based on
pipeline_production_date = '2024-05-03'


# -----------------------------------------------------------------------------
# Datasets
# -----------------------------------------------------------------------------
# data frame of datasets
datasets = [
  # -----------------------------------------------------------------------------
  # Datasets requested by the project
  # -----------------------------------------------------------------------------  
    ['gdppr',         dbc, f'gdppr_{db}_archive',                   'NHS_NUMBER_DEID',                  'DATE']  
  , ['hes_apc',       dbc, f'hes_apc_all_years_archive',            'PERSON_ID_DEID',                   'EPISTART']
  , ['deaths',        dbc, f'deaths_{db}_archive',                  'DEC_CONF_NHS_NUMBER_CLEAN_DEID',   'REG_DATE_OF_DEATH']
  
  
  # -----------------------------------------------------------------------------
  # Additonal datasets needed for the data curation pipeline for this project
  # -----------------------------------------------------------------------------
  , ['hes_ae',        dbc, f'hes_ae_all_years_archive',             'PERSON_ID_DEID',                   'ARRIVALDATE']
  , ['hes_op',        dbc, f'hes_op_all_years_archive',             'PERSON_ID_DEID',                   'APPTDATE']
  , ['hes_cc',        dbc, f'hes_cc_all_years_archive',             'PERSON_ID_DEID',                   'CCSTARTDATE'] 
  , ['pmeds',         dbc, f'primary_care_meds_{db}_archive',       'Person_ID_DEID',                   'ProcessingPeriodDate']           
  
  # -----------------------------------------------------------------------------
  # Datasets not required for this project
  # -----------------------------------------------------------------------------  

# , ['chess',         dbc, f'chess_{db}_archive',                   'PERSON_ID_DEID',                   'InfectionSwabDate']
# , ['pillar2',       dbc, f'covid_antigen_testing_pillar2_{db}_archive',         'Person_ID_DEID',                 'TestStartDate']  
# , ['sgss',          dbc, f'sgss_{db}_archive',                    'PERSON_ID_DEID',                   'Specimen_Date']
# , ['sus',           dbc, f'sus_{db}_archive',                     'NHS_NUMBER_DEID',                  'EPISODE_START_DATE']
# , ['icnarc',        dbc, f'icnarc_{db}_archive',                  'NHS_NUMBER_DEID',                  'Date_of_admission_to_your_unit']
# , ['nacsa',         dbc, f'nicor_acs_combined_{db}_archive',      'NHS_NUMBER_DEID',                  'ADMISSION_DATE'  ] 
#   , ['ssnap',         dbc, f'ssnap_{db}_archive',             'PERSON_ID_DEID',                'S1ONSETDATETIME'] 
#   , ['minap',         dbc, f'minap_{db}_archive',             'NHS_NUMBER_DEID',               'ARRIVAL_AT_HOSPITAL'] 
#   , ['nhfa',          dbc, f'nhfa_{db}_archive',              '1_03_NHS_NUMBER_DEID',          '2_00_DATE_OF_VISIT'] 
#   , ['nvra',          dbc, f'nvra_{db}_archive',              'NHS_NUMBER_DEID',               'DATE'] 
#   , ['vacc',          dbc, f'vaccine_status_{db}_archive',    'PERSON_ID_DEID',                'DATE_AND_TIME']  
]

tmp_df_datasets = pd.DataFrame(datasets, columns=['dataset', 'database', 'table', 'id', 'date']).reset_index()

if(run_all_toggle):
  print('tmp_df_datasets:\n', tmp_df_datasets.to_string())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Datasets Archived States

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.1 Create

# COMMAND ----------

# DBTITLE 1,All archived states
# for each dataset in tmp_df_datasets, 
#   tabulate all archived_on dates (for information)
#   find the latest (most recent) archived_on date before the pipeline_production_date
#   create a table containing a row with the latest archived_on date and count of the number of records for each dataset
  
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):

  latest_archived_on = []
  lsoa_1st = []
  for index, row in tmp_df_datasets.iterrows():
    # initial  
    dataset = row['dataset']
    path = row['database'] + '.' + row['table']
    print(index, dataset, path); print()

    # point to table
    tmpd = spark.table(path)

    # tabulate all archived_on dates
    tmpt = tab(tmpd, 'archived_on')
    
    # extract latest (most recent) archived_on date before the pipeline_production_date
    tmpa = (
      tmpd
      .groupBy('archived_on')
      .agg(f.count(f.lit(1)).alias('n'))
      .withColumn('n', f.format_number('n', 0))
      .where(f.col('archived_on') <= pipeline_production_date)
      .orderBy(f.desc('archived_on'))
      .limit(1)
      .withColumn('dataset', f.lit(dataset))
      .select('dataset', 'archived_on', 'n')
    )
    
    # extract closest archived_on date that comes after study_start_date
    if(dataset=="gdppr"):
      tmpb = (
        tmpd
        .groupBy('archived_on')
        .agg(f.count(f.lit(1)).alias('n'))
        .withColumn('n', f.format_number('n', 0))
        .where(f.col('archived_on') >= study_start_date_primary)
        .orderBy(f.asc('archived_on'))
        .limit(1)
        .withColumn('dataset', f.lit(dataset))
        .select('dataset', 'archived_on', 'n')
      )
      
      if(index == 0): lsoa_1st = tmpb
      else: lsoa_1st = lsoa_1st.unionByName(tmpb)
    
    # append results
    if(index == 0): latest_archived_on = tmpa
    else: latest_archived_on = latest_archived_on.unionByName(tmpa)
    print()
    


  # check
  print('Latest (most recent) archived_on date before pipeline_production_date')
  print(latest_archived_on.toPandas().to_string())
  print('\nClosest (1st) GDPPR archived_on date following study_start_date')
  print(lsoa_1st.toPandas().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.2 Check

# COMMAND ----------

# DBTITLE 1,Latest archived states
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  # check
  display(latest_archived_on)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.3 Prepare

# COMMAND ----------

# prepare the tables to be saved

# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  
  # merge the datasets dataframe with the latest_archived_on
  tmp_df_datasets_sp = spark.createDataFrame(tmp_df_datasets) 
  #parameters_df_datasets = merge(tmp_df_datasets_sp, latest_archived_on, ['dataset'], validate='1:1', #assert_results=['both'], indicator=0).orderBy('index'); print()

  parameters_df_datasets = merge(tmp_df_datasets_sp, latest_archived_on, ['dataset']).orderBy('index'); print()
  
  # check  
  print(parameters_df_datasets.toPandas().to_string())


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.4 Save

# COMMAND ----------

# save the parameters_df_datasets table
# which we can simply import below when not running all and calling this notebook in subsequent notebooks

# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  save_table(df=parameters_df_datasets, out_name=f'{proj}_parameters_df_datasets', save_previous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.5 Import

# COMMAND ----------

# import the parameters_df_datasets table 
# convert to a Pandas dataframe and transform archived_on to a string (to conform to the input that the extract_batch_from_archive function is expecting)

spark.sql(f'REFRESH TABLE {dsa}.{proj}_parameters_df_datasets')
parameters_df_datasets = (
  spark.table(f'{dsa}.{proj}_parameters_df_datasets')
  .orderBy('index')
  .toPandas()
)
parameters_df_datasets['archived_on'] = parameters_df_datasets['archived_on'].astype(str)

# COMMAND ----------

display(parameters_df_datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Curated Data Paths

# COMMAND ----------

# -----------------------------------------------------------------------------
# These are paths to data tables curated in subsequent notebooks that may be
# needed in subsequent notebooks from which they were curated
# -----------------------------------------------------------------------------

# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_ref_opcs4           = 'dss_corporate.opcs_codes_v02'
path_ref_person_id_type_lkp   = f'{db}.token_pseudo_id_lookup'
# path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
# path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
# path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'

# curated tables 
path_cur_hes_apc_long      = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long'
#path_cur_hes_apc_oper_long = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dsa}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dsa}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dsa}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dsa}.{proj}_cur_lsoa_imd_lookup'
path_cur_lsoa              = f'{dsa}.{proj}_lsoa'
#path_cur_covid             = f'{dsa}.{proj}_cur_covid'
#path_nacsa_washout         = f'{dsa}.{proj}_nacsa_washout'

# path_cur_vacc_first        = f'{dbc}.{proj}_cur_vacc_first'
# path_cur_covid             = f'{dbc}.{proj}_cur_covid'

# # temporary tables
#path_tmp_skinny_unassembled             = f'{dsa}.{proj}_tmp_kpc_harmonised_1'
#path_tmp_skinny_assembled               = f'{dsa}.{proj}_tmp_kpc_selected'
path_tmp_skinny                         = f'{dsa}.hds_curated_assets_demographics_20240425'

path_tmp_quality_assurance_hx_1st_wide  = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dsa}.{proj}_tmp_quality_assurance_qax'
path_tmp_quality_assurance              = f'{dsa}.{proj}_tmp_quality_assurance'

path_tmp_inc_exc_cohort                 = f'{dsa}.{proj}_tmp_inc_exc_cohort'
path_tmp_inc_exc_flow                   = f'{dsa}.{proj}_tmp_inc_exc_flow'

# out tables
## generic codelist file paths
path_out_codelist_quality_assurance      = f'{dsa}.{proj}_out_codelist_quality_assurance'
path_out_codelist_covid                  = f'{dsa}.{proj}_out_codelist_covid'
path_out_codelist_exposure             = f'{dsa}.{proj}_out_codelist_exposure'
path_out_codelist_outcomes               = f'{dsa}.{proj}_out_codelist_outcomes'
path_out_codelist_covariates_markers     = f'{dsa}.{proj}_out_codelist_covariates_markers'
## codelists provided by project team
path_codelist_sct_schizophrenia          = f'{dsa}.{proj}_codelists_4jul2024_snomed_schizophrenia'
path_codelist_sct_bipolar              = f'{dsa}.{proj}_codelists_4jul2024_snomed_bipolar_disorder'
path_codelist_sct_depression             = f'{dsa}.{proj}_codelists_4jul2024_snomed_depression'
path_codelist_sct_heart_failure         = f'{dsa}.{proj}_codelists_23jul2024_snomed_heart_failure'
path_codelist_sct_myocardial_infarction         = f'{dsa}.{proj}_codelists_23jul2024_snomed_myocardial_infarction'
path_codelist_sct_stroke         = f'{dsa}.{proj}_codelists_23jul2024_snomed_stroke'
# paths to key outputs
path_out_cohort                          = f'{dsa}.{proj}_out_cohort'
path_out_exposures                       = f'{dsa}.{proj}_out_exposures'
path_out_outcomes_hes_apc                = f'{dsa}.{proj}_out_outcomes_hes_apc'
path_out_combined                        = f'{dsa}.{proj}_out_combined'
