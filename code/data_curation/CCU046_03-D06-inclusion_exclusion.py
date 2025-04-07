# Databricks notebook source
# MAGIC %md # CCU046_03-D06-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook applies the inclusion/exclusion criteria.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D06-inclusion_exclusion
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu046_03_tmp_inc_exc_cohort`** : cohort remaining after inclusion/exclusion criteria applied
# MAGIC - **`ccu0456_03_tmp_inc_exc_flow`** : flowchart displaying total n of cohort after each inclusion/exclusion rule is applied

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

# DBTITLE 1,Functions
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

# MAGIC %md # 1. Data

# COMMAND ----------


# Datasets
skinny        = spark.table(f'{dsa}.hds_curated_assets_demographics_20240425')
deaths        = spark.table(path_cur_deaths_sing)
qa            = spark.table(f'{dsa}.{proj}_tmp_quality_assurance')

# Lookup tables
lsoa_region_lookup = spark.table(path_cur_lsoa_region)
lsoa_imd_lookup = spark.table(path_cur_lsoa_imd)

# COMMAND ----------

display(skinny)

# COMMAND ----------

display(deaths)

# COMMAND ----------

display(qa)

# COMMAND ----------

display(lsoa_region_lookup)

# COMMAND ----------

display(lsoa_imd_lookup)

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
skinny_prepared = (skinny.select('PERSON_ID', 'date_of_birth', 'sex', 'sex_code', 
                                'ethnicity_5_group', 'ethnicity_19_code', 'ethnicity_19_group', 'ethnicity_raw_code', 'ethnicity_raw_description','in_gdppr', 'lsoa', 'imd_decile', 'imd_quintile', 'region',
                                'death_flag', 'date_of_death')
                   .withColumnRenamed('lsoa', 'LSOA'))

# check
count_var(skinny_prepared, 'PERSON_ID'); print()

print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
qa_prepared = (
  qa
  .withColumn('in_qa', f.lit(1))
)

# check
count_var(qa_prepared, 'PERSON_ID'); print()
tmpt = tab(qa_prepared, '_rule_concat', '_rule_total', var2_unstyled=1); print()


# use skinny table as starting point
_merged = skinny_prepared

# merge in qa
_merged = merge(_merged, qa_prepared, ['PERSON_ID'], validate='m:1', 
                keep_results=['both', 'left_only'], indicator=0); print()

# add baseline_date (study start date)
_merged = (
    _merged
    .withColumn('study_start_date_primary', f.to_date(f.lit(study_start_date_primary)))
    .withColumn('study_start_date_secondary', f.to_date(f.lit(study_start_date_secondary)))
    .withColumn('study_end_date', f.to_date(f.lit(study_end_date)))
)


# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# temp save
_merged = temp_save(df=_merged, out_name=f'{proj}_tmp_inc_exc_merged'); print()

# check
count_var(_merged, 'PERSON_ID'); print()

# COMMAND ----------

# check
tmpt = tab(_merged, 'in_gdppr'); print()
#tmpt = tab(_merged, 'in_deaths'); print()
tmpt = tab(_merged, 'in_qa'); print()

tmpt = tab(_merged, '_rule_total'); print()
tmpt = tab(_merged, '_rule_concat', '_rule_total', var2_unstyled=1); print()

# tmpt = tab(_merged, 'in_sgss'); print()

# COMMAND ----------

# MAGIC %md # 3. Inclusion / exclusion

# COMMAND ----------

# MAGIC %md ## 3.1 Create inclusion flags

# COMMAND ----------


# Create inclusion flags
_merged = (
    _merged
    .withColumn(
        'with_valid_id',
        f.when(f.col('PERSON_ID').isNotNull(), f.lit(True))
        .otherwise(False)
    )
    .withColumn(
        'lsoa_in_england_or_missing',
        f.when(
            (f.col('LSOA').isNull()) | (f.col('LSOA').startswith('E')) ,
            f.lit(True)
        )
        .otherwise(False)
    )
    .withColumn(
        'passed_quality_check',
        f.when(f.col('_rule_total') == f.lit(0), f.lit(True))
        .otherwise(False)
    )
)

_merged = (
    _merged
    .withColumn('c0', f.lit(True))
    .withColumn('c1', f.col('c0') & f.col('with_valid_id'))
    .withColumn('c2', f.col('c1') & f.col('lsoa_in_england_or_missing'))
    .withColumn('c3', f.col('c2') & f.col('passed_quality_check'))
    .withColumn('include', f.col('c3'))
)

display(_merged)

# COMMAND ----------

tab(_merged, 'c3')

# COMMAND ----------

# MAGIC %md ## 3.2 Flowchart

# COMMAND ----------

# Criteria list
criteria_list = ['c0', 'c1', 'c2', 'c3']

# Count number satisfying each criteria
flowchart = (
    _merged
    .select(['PERSON_ID'] + criteria_list)
    .select(
        [f.count(f.when(_merged[c] == True, 1)).alias('n_records_' + c) for c in criteria_list] +
        [f.countDistinct(f.when(_merged[c] == True, f.col('PERSON_ID'))).alias('n_id_' + c) for c in criteria_list]
    )
)

# Create dummy column to pivot longer
flowchart = (
    flowchart
    .withColumn('_dummy', f.lit(1))
)

# Pivot longer, label criteria
flowchart = (
    reshape_wide_to_long_multi(flowchart, i = ['_dummy'], j = 'criteria', stubnames = ['n_records_', 'n_id_'])
    .drop('_dummy')
    .withColumnRenamed('n_records_', 'n_records')
    .withColumnRenamed('n_id_', 'n_id')
    .withColumn('n_records_excl', f.lag('n_records', 1).over(Window.orderBy('criteria')) - f.col('n_records'))
    .withColumn('n_id_excl', f.lag('n_id', 1).over(Window.orderBy('criteria')) - f.col('n_id'))
    .withColumn(
        'criteria_description',
        f.when(f.col('criteria') == f.lit('c0'), f'Records in patient characteristics table')
        .when(f.col('criteria') == f.lit('c1'), f' - valid patient ID')
        .when(f.col('criteria') == f.lit('c2'), f' - LSOA unknown or in England')
        .when(f.col('criteria') == f.lit('c3'), f' - passed quality assurance checks')
    )
)


# COMMAND ----------

display(flowchart)

# COMMAND ----------

# MAGIC %md ## 3.3 Save flowchart

# COMMAND ----------

# Save flowchart
save_table(df=flowchart, out_name=f'{proj}_tmp_inc_exc_flow', save_previous=True)

# Reload
flowchart = spark.table(path_tmp_inc_exc_flow)

# Display flowchart
display(flowchart)

# COMMAND ----------

# MAGIC %md # 4. Cohort

# COMMAND ----------

# MAGIC %md ## 4.1 Apply exclusion criteria

# COMMAND ----------

cohort = (
    _merged
    .filter(f.col('include') == f.lit(True))
)

# COMMAND ----------

# MAGIC %md ## 4.2 Select columns

# COMMAND ----------

cohort = (
    cohort
    .select(
        'PERSON_ID',
        'study_start_date_primary', 'study_start_date_secondary', 'study_end_date',
        'date_of_birth', 'sex', 'sex_code',
        'ethnicity_5_group', 'ethnicity_19_code', 'ethnicity_19_group', 'ethnicity_raw_code', 'ethnicity_raw_description','in_gdppr', 'lsoa', 'imd_decile', 'imd_quintile', 'region',
        'death_flag', 'date_of_death',

    )
)


# COMMAND ----------

# MAGIC %md ## 4.3 Save cohort

# COMMAND ----------

save_table(df=cohort, out_name=f'{proj}_tmp_inc_exc_cohort', save_previous = False)

# COMMAND ----------

# MAGIC %md ## 4.4 Checks

# COMMAND ----------

# Load and display
cohort = spark.table(path_tmp_inc_exc_cohort)
display(cohort)

# COMMAND ----------

count_var(cohort, 'PERSON_ID'); print()