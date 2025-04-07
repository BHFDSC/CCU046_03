# Databricks notebook source
# MAGIC %md # CCU046_03-D07-outcomes
# MAGIC  
# MAGIC **Description** This notebook identifies the first occurrence of each outcome of interest for all patients in the cohort table. These flags will be used to exclude patients with a historic event prior to the primary/secondary analysis start dates, and also to identify incident events for each month during study period.
# MAGIC  
# MAGIC **Authors** John Nolan (Health Data Science Team, BHF Data Science Centre)
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** 
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu046_03_out_outcomes_primary`** : table of outcomes derived from HES-APC and GDPPR
# MAGIC - **`ccu046_03_out_outcomes_secondary`** : table of outcomes derived from HES-APC only

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Function
# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_03-D01-parameters"

# COMMAND ----------

# widgets
#dbutils.widgets.removeAll()
#dbutils.widgets.text('1 project', proj)
#dbutils.widgets.text('2 cohort', cohort)
#dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# Data tables
# here we will use the patient demographics table as the cohort
cohort      = spark.table(path_tmp_inc_exc_cohort)
hes_apc_long = spark.table(path_cur_hes_apc_long)
gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
deaths = spark.table(path_cur_deaths_long)

# Look-up table
icd10_lookup = spark.table(path_ref_icd10)

# COMMAND ----------

#gdppr = gdppr.limit(1000000)
#hes_apc_long = hes_apc_long.limit(1000000)
#deaths = deaths.limit(1000000)

# COMMAND ----------

# save dev tables

# gdppr
#outName = 'ccu046_03_dev_gdppr'
#gdppr.write.mode('overwrite').saveAsTable(f'{dsa}.{outName}')

# hes_apc_long
#outName = 'ccu046_03_dev_hes_apc_long'
#hes_apc_long.write.mode('overwrite').saveAsTable(f'{dsa}.{outName}')

# deaths
#outName = 'ccu046_03_dev_deaths'
#deaths.write.mode('overwrite').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# reload working tables
# gdppr
#outName = 'ccu046_03_dev_gdppr'
#gdppr = spark.table(f'{dsa}.{outName}')

# hes_apc_long
#outName = 'ccu046_03_dev_hes_apc_long'
#hes_apc_long = spark.table(f'{dsa}.{outName}')

# deaths
#outName = 'ccu046_03_dev_deaths'
#deaths = spark.table(f'{dsa}.{outName}')

# COMMAND ----------

display(cohort)

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

display(deaths)

# COMMAND ----------

display(icd10_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 Cohort

# COMMAND ----------

cohort_prepared = (
    cohort
    .withColumnRenamed('date_of_birth', 'CENSOR_DATE_START')
    .withColumn('CENSOR_DATE_END', f.to_date(f.lit(study_end_date)))
    .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')
)

# COMMAND ----------

display(cohort_prepared)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.2 GDPPR, HES-APC and Deaths

# COMMAND ----------

gdppr_prepared = (gdppr
         .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
         .select(['PERSON_ID', 'CODE', 'DATE'])
         .join(cohort_prepared, on = 'PERSON_ID', how = 'inner')
         .where(
             (f.col('DATE') > f.col('CENSOR_DATE_START'))
             & (f.col('DATE') <= f.col('CENSOR_DATE_END')))
         )

# COMMAND ----------

hes_apc_long_prepared = (hes_apc_long
                .withColumnRenamed('EPISTART', 'DATE')
                .select(['PERSON_ID', 'CODE', 'DATE'])
                .join(cohort_prepared, on = 'PERSON_ID', how = 'inner')
                .where(
                    (f.col('DATE') > f.col('CENSOR_DATE_START'))
                    & (f.col('DATE') <= f.col('CENSOR_DATE_END')))
                )              

# COMMAND ----------

deaths_prepared = (deaths
          .select(['PERSON_ID', 'CODE', 'DATE'])
          .join(cohort_prepared, on = 'PERSON_ID', how = 'inner')
                .where(
                    (f.col('DATE') > f.col('CENSOR_DATE_START'))
                    & (f.col('DATE') <= f.col('CENSOR_DATE_END')))
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Outcomes Codelist

# COMMAND ----------

# updated snomed codelsists for outcomes provided by Kelly Fleetwood in October 2024
codelist_sct_stroke = spark.table(path_codelist_sct_stroke)
codelist_sct_myocardial_infarction = spark.table(path_codelist_sct_myocardial_infarction)
codelist_sct_heart_failure = spark.table(path_codelist_sct_heart_failure)

codelist_sct = (codelist_sct_stroke.withColumn('condition', f.lit('stroke'))
                .unionByName(codelist_sct_myocardial_infarction.withColumn('condition', f.lit('ami')))
                .unionByName(codelist_sct_heart_failure.withColumn('condition', f.lit('heart_failure')))
)
display(codelist_sct)

# COMMAND ----------

# ICD10 codelist:
codelist_icd10 = """
name,code
ami,I21
ami,I22
heart_failure,I50
heart_failure,I110
heart_failure,I130
heart_failure,I132
stroke,I60
stroke,I61
stroke,I63
stroke,I64
"""
codelist_icd10 = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_icd10)))
    .fillna('')
    .astype(str)
  )
)

# merge to ICD10 lookup
codelist_icd10 = codelist_icd10\
  .join(icd10_lookup
        .select('ALT_CODE', 'ICD10_DESCRIPTION')
        .withColumn('ALT_CODE', f.when(f.col('ALT_CODE') == "I64X", f.lit("I64")).otherwise(f.col('ALT_CODE')))
        .withColumnRenamed('ALT_CODE', 'code')
        .withColumnRenamed('ICD10_DESCRIPTION', 'term')
        .select('code', 'term')
        .withColumn('inICD10', f.lit(1)),
        on = 'code',
        how = 'left')\
  .withColumn('inICD10', f.when(f.col('inICD10').isNull(), f.lit(0)).otherwise(f.col('inICD10')))\
  .select('name', 'code', 'term', 'inICD10')

# check
tab(codelist_icd10, 'inICD10')

# final
codelist_icd10 = codelist_icd10\
  .drop('inICD10')

# display
display(codelist_icd10)

# COMMAND ----------

# create prepared codelists for matching
codelist_icd10 = (codelist_icd10
                  .select(['name', 'code', 'term'])
                  .withColumn('terminology', f.lit('ICD10')))
codelist_sct = (codelist_sct
                .withColumnRenamed('condition', 'name')
                .select(['name', 'code', 'description'])
                .withColumn('terminology', f.lit('SNOMED'))
                .withColumnRenamed('description', 'term'))

# COMMAND ----------

display(codelist_icd10)

# COMMAND ----------

display(codelist_sct)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Codelist Match

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist Match Primary Analysis (GDPPR, HES APC, Deaths)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_out = {
    'hes_apc_long':             ['hes_apc_long_prepared',         'codelist_icd10', 1]
  , 'gdppr':               ['gdppr_prepared',             'codelist_sct',  2]
  , 'deaths':              ['deaths_prepared',          'codelist_icd10', 3]
 }

# run codelist match and codelist match summary functions
out, out_1st, out_1st_wide = codelist_match(dict_out, _name_prefix=f'out_')
out_summ_name, out_summ_name_code = codelist_match_summ(dict_out, out)

# temp save
out_all = out['all']
out_all = temp_save(df=out_all, out_name=f'{proj}_tmp_outcomes_all_sources_out_all'); print()
out_1st = temp_save(df=out_1st, out_name=f'{proj}_tmp_outcomes_all_sources_out_1st'); print()
out_1st_wide = temp_save(df=out_1st_wide, out_name=f'{proj}_tmp_outcomes_all_sources_out_1st_wide'); print()
out_summ_name = temp_save(df=out_summ_name, out_name=f'{proj}_tmp_outcomes_all_sources_out_summ_name'); print()
out_summ_name_code = temp_save(df=out_summ_name_code, out_name=f'{proj}_tmp_outcomes_all_sources_out_summ_name_code'); print

# COMMAND ----------

# MAGIC %md ## 3.2 Codelist Match Secondary Analysis (HES APC, Deaths)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_out = {
    'hes_apc_long':             ['hes_apc_long_prepared',         'codelist_icd10', 1]
    , 'deaths':              ['deaths_prepared',          'codelist_icd10', 2]
 }

# run codelist match and codelist match summary functions
out, out_1st, out_1st_wide = codelist_match(dict_out, _name_prefix=f'out_')
out_summ_name, out_summ_name_code = codelist_match_summ(dict_out, out)

# temp save
out_all = out['all']
out_all = temp_save(df=out_all, out_name=f'{proj}_tmp_outcomes_secondary_sources_out_all'); print()
out_1st = temp_save(df=out_1st, out_name=f'{proj}_tmp_outcomes_secondary_sources_out_1st'); print()
out_1st_wide = temp_save(df=out_1st_wide, out_name=f'{proj}_tmp_outcomes_secondary_sources_out_1st_wide'); print()
out_summ_name = temp_save(df=out_summ_name, out_name=f'{proj}_tmp_outcomes_secondary_sources_out_summ_name'); print()
out_summ_name_code = temp_save(df=out_summ_name_code, out_name=f'{proj}_tmp_outcomes_secondary_sources_out_summ_name_code'); print

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Create final outcome tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Primary analysis

# COMMAND ----------

out_primary = spark.table(f'{dsa}.{proj}_tmp_outcomes_all_sources_out_1st_wide')

# COMMAND ----------

display(out_primary)

# COMMAND ----------

# add any cvd outcome flags and dates
out_primary_final = (out_primary
                     .drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
                     .withColumn('out_any_flag', f.when((f.col('out_ami_flag') == 1) | (f.col('out_heart_failure_flag') == 1) | (f.col('out_stroke_flag') == 1), f.lit(1)))
                     .withColumn('out_any_date', f.least(f.col('out_ami_date'), f.col('out_heart_failure_date'), f.col('out_stroke_date')))
               )

# COMMAND ----------

# rename columns
out_columns = out_primary_final.columns
for i in range(len(out_columns)):
    out_primary_final = (out_primary_final
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('_flag','_primary_flag'))
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('_date','_primary_date'))
    )


# COMMAND ----------

display(out_primary_final)

# COMMAND ----------

# MAGIC %md ## 4.2 Secondary analysis

# COMMAND ----------

out_secondary = spark.table(f'{dsa}.{proj}_tmp_outcomes_secondary_sources_out_1st_wide')

# COMMAND ----------

# add any cvd outcome flags and dates
out_secondary_final = (out_secondary
                       .drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
                       .withColumn('out_any_flag', f.when((f.col('out_ami_flag') == 1) | (f.col('out_heart_failure_flag') == 1) | (f.col('out_stroke_flag') == 1), f.lit(1)))
                       .withColumn('out_any_date', f.least(f.col('out_ami_date'), f.col('out_heart_failure_date'), f.col('out_stroke_date')))
                       )

# COMMAND ----------

# rename columns
out_columns = out_secondary_final.columns
for i in range(len(out_columns)):
    out_secondary_final = (out_secondary_final
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('_flag','_secondary_flag'))
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('_date','_secondary_date'))
    )

# COMMAND ----------

display(out_secondary_final)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# MAGIC %md ## 5.1 Primary analysis

# COMMAND ----------

save_table(df = out_primary_final, out_name = f'{proj}_out_outcomes_primary', save_previous=True)

# COMMAND ----------

# MAGIC %md ## 5.2 Secondary analysis

# COMMAND ----------

save_table(df = out_secondary_final, out_name = f'{proj}_out_outcomes_secondary', save_previous=True)

# COMMAND ----------

# MAGIC %md # 6 Check

# COMMAND ----------

# Load table
outcomes_primary = spark.table(f'{dsa}.{proj}_out_outcomes_primary')

# Display table
display(outcomes_primary)

# COMMAND ----------

count_var(outcomes_primary, 'PERSON_ID')

# COMMAND ----------

# Load table
outcomes_secondary = spark.table(f'{dsa}.{proj}_out_outcomes_secondary')

# Display table
display(outcomes_secondary)

# COMMAND ----------

count_var(outcomes_secondary, 'PERSON_ID')