# Databricks notebook source
# MAGIC %md # CCU046_03-D08-exposure
# MAGIC  
# MAGIC **Description** This notebook creates the SMI exposure flags. There are 2 sets of flags, one for primary analysis (SMI identified from GDPPR + HES APC), and one for secondary analysis (HES APC only)
# MAGIC  
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, Alexia Sampri for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

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

# DBTITLE 1,Functions
# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_03-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist_exp  = spark.table(path_out_codelist_exposure)
cohort       = spark.table(path_tmp_inc_exc_cohort)
gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc_long = spark.table(path_cur_hes_apc_long)


# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Prepare cohort lookup with required date fields

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
individual_censor_dates = (cohort
                           .select('PERSON_ID', 'date_of_birth')
                           .withColumnRenamed('date_of_birth', 'CENSOR_DATE_START')
                           .withColumn('CENSOR_DATE_END', f.to_date(f.lit(study_end_date))))

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Prepare GDPPR data

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_prepared = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# add individual censor dates
gdppr_prepared = (
  gdppr_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_prepared = (
  gdppr_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()
# print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_covariates_gdppr_tb')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Prepare HES APC data

# COMMAND ----------

# MAGIC %md ### 2.3.1 Diag

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_apc_long_prepared = (
  hes_apc_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check 1
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_apc_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# print(hes_apc_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_hes_apc_tb')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 Exposure (SMI)

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

codelist_exp = codelist_exp\
  .withColumn('name', f.when(f.col('name') == 'Bipolar disorder', 'Bipolar_disorder').otherwise(f.col('name')))
display(codelist_exp)

# COMMAND ----------

# check
tmpt = tab(codelist_exp, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Create Primary

# COMMAND ----------

# MAGIC %md ### 3.2.1 Codelist match

# COMMAND ----------

hes_apc_long_prepared = hes_apc_long_prepared\
  .drop('DIAG_POSITION', 'DIAG_DIGITS')
display(hes_apc_long_prepared)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_exp = {
    'hes_apc':  ['hes_apc_long_prepared',  'codelist_exp',  1]
  , 'gdppr':        ['gdppr_prepared',             'codelist_exp', 2]
}

# run codelist match and codelist match summary functions
hx_exp, hx_exp_1st, hx_exp_1st_wide = codelist_match(dict_hx_exp, _name_prefix=f'cov_hx_exp_'); print()
hx_exp_summ_name, hx_exp_summ_name_code = codelist_match_summ(dict_hx_exp, hx_exp); print()


# temp save
hx_exp = hx_exp['all']
hx_exp = temp_save(df=hx_exp, out_name=f'{proj}_tmp_exposure_all_sources_exp_all'); print()
hx_exp_1st = temp_save(df=hx_exp_1st, out_name=f'{proj}_tmp_exposure_all_sources_exp_1st'); print()
hx_exp_1st_wide = temp_save(df=hx_exp_1st_wide, out_name=f'{proj}_tmp_exposure_all_sources_exp_1st_wide'); print()
hx_exp_summ_name = temp_save(df=hx_exp_summ_name, out_name=f'{proj}_tmp_exposure_all_sources_exp_summ_name'); print()
hx_exp_summ_name_code = temp_save(df=hx_exp_summ_name_code, out_name=f'{proj}_tmp_exposure_all_sources_exp_summ_name_code'); print

# COMMAND ----------

# MAGIC %md ###3.2.2 Create final table

# COMMAND ----------

exp_primary = spark.table(f'{dsa}.{proj}_tmp_exposure_all_sources_exp_1st_wide')

# COMMAND ----------

display(exp_primary)

# COMMAND ----------

# create primary SMI variables
exp_primary_final = (exp_primary
                     .drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
                     .withColumn('cov_hx_exp_smi_label', f.when(f.col('cov_hx_exp_schizophrenia_flag') == 1, 'schizophrenia')
                                 .when(f.col('cov_hx_exp_bipolar_disorder_flag') == 1, 'bipolar_disorder')
                                 .when(f.col('cov_hx_exp_depression_flag') == 1, 'depression'))
                     .withColumn('cov_hx_exp_smi_date', f.when(f.col('cov_hx_exp_schizophrenia_flag') == 1, f.col('cov_hx_exp_schizophrenia_date'))
                                 .when(f.col('cov_hx_exp_bipolar_disorder_flag') == 1, f.col('cov_hx_exp_bipolar_disorder_date'))
                                 .when(f.col('cov_hx_exp_depression_flag') == 1, f.col('cov_hx_exp_depression_date')))
)

# COMMAND ----------

# rename columns
out_columns = exp_primary_final.columns
for i in range(len(out_columns)):
    exp_primary_final = (exp_primary_final
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('cov_hx_','primary_'))
                         )

# COMMAND ----------

tab(exp_primary_final, 'primary_exp_smi_label')
tab(exp_primary_final, 'primary_exp_smi_label', 'primary_exp_depression_flag')
tab(exp_primary_final, 'primary_exp_smi_label', 'primary_exp_bipolar_disorder_flag')
tab(exp_primary_final, 'primary_exp_smi_label', 'primary_exp_schizophrenia_flag')

# COMMAND ----------

display(exp_primary_final)

# COMMAND ----------

count_var(exp_primary_final, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ## 3.3 Create Secondary

# COMMAND ----------

# MAGIC %md ### 3.3.1 Codelist match

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_exp = {
    'hes_apc':  ['hes_apc_long_prepared',  'codelist_exp',  1]
}

# run codelist match and codelist match summary functions
hx_exp, hx_exp_1st, hx_exp_1st_wide = codelist_match(dict_hx_exp, _name_prefix=f'cov_hx_exp_'); print()
hx_exp_summ_name, hx_exp_summ_name_code = codelist_match_summ(dict_hx_exp, hx_exp); print()

# temp save
hx_exp = hx_exp['all']
hx_exp = temp_save(df=hx_exp, out_name=f'{proj}_tmp_exposure_secondary_sources_exp_all'); print()
hx_exp_1st = temp_save(df=hx_exp_1st, out_name=f'{proj}_tmp_exposure_secondary_sources_exp_1st'); print()
hx_exp_1st_wide = temp_save(df=hx_exp_1st_wide, out_name=f'{proj}_tmp_exposure_secondary_sources_exp_1st_wide'); print()
hx_exp_summ_name = temp_save(df=hx_exp_summ_name, out_name=f'{proj}_tmp_exposure_secondary_sources_exp_summ_name'); print()
hx_exp_summ_name_code = temp_save(df=hx_exp_summ_name_code, out_name=f'{proj}_tmp_exposure_secondary_sources_exp_summ_name_code'); print

# COMMAND ----------

# MAGIC %md ### 3.3.2 Create final table

# COMMAND ----------

exp_secondary = spark.table(f'{dsa}.{proj}_tmp_exposure_secondary_sources_exp_1st_wide')

# COMMAND ----------

display(exp_secondary)

# COMMAND ----------

# create secondary SMI variables
exp_secondary_final = (exp_secondary
                     .drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
                     .withColumn('cov_hx_exp_smi_label', f.when(f.col('cov_hx_exp_schizophrenia_flag') == 1, 'schizophrenia')
                                 .when(f.col('cov_hx_exp_bipolar_disorder_flag') == 1, 'bipolar_disorder')
                                 .when(f.col('cov_hx_exp_depression_flag') == 1, 'depression'))
                     .withColumn('cov_hx_exp_smi_date', f.when(f.col('cov_hx_exp_schizophrenia_flag') == 1, f.col('cov_hx_exp_schizophrenia_date'))
                                 .when(f.col('cov_hx_exp_bipolar_disorder_flag') == 1, f.col('cov_hx_exp_bipolar_disorder_date'))
                                 .when(f.col('cov_hx_exp_depression_flag') == 1, f.col('cov_hx_exp_depression_date')))
)

# COMMAND ----------

# rename columns
out_columns = exp_secondary_final.columns
for i in range(len(out_columns)):
    exp_secondary_final = (exp_secondary_final
                         .withColumnRenamed(out_columns[i], out_columns[i].replace('cov_hx_','secondary_'))
                         )

# COMMAND ----------

tab(exp_secondary_final, 'secondary_exp_smi_label')
tab(exp_secondary_final, 'secondary_exp_smi_label', 'secondary_exp_depression_flag')
tab(exp_secondary_final, 'secondary_exp_smi_label', 'secondary_exp_bipolar_disorder_flag')
tab(exp_secondary_final, 'secondary_exp_smi_label', 'secondary_exp_schizophrenia_flag')

# COMMAND ----------

display(exp_secondary_final)

# COMMAND ----------

count_var(exp_secondary_final, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# MAGIC %md ##4.1 Primary analysis (GDPPR and HES APC)

# COMMAND ----------

save_table(df = exp_primary_final, out_name = f'{proj}_out_exposure_primary', save_previous=True)

# COMMAND ----------

# MAGIC %md ##4.2 Secondary analysis (HES APC)

# COMMAND ----------

save_table(df = exp_secondary_final, out_name = f'{proj}_out_exposure_secondary', save_previous=True)