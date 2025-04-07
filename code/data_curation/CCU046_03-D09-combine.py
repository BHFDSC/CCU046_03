# Databricks notebook source
# MAGIC %md
# MAGIC # CCU046_03-D09-combine
# MAGIC
# MAGIC **Description** This notebook combines the cohort, exposure and outcomes tables.
# MAGIC
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU003_05
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_03-D01-parameters" 

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# cohort
spark.sql(f'REFRESH TABLE {dsa}.{proj}_tmp_inc_exc_cohort')
cohort = spark.table(f'{dsa}.{proj}_tmp_inc_exc_cohort')

# exposure
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_exposure_primary')
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_exposure_secondary')
exp_primary = spark.table(f'{dsa}.{proj}_out_exposure_primary')
exp_secondary = spark.table(f'{dsa}.{proj}_out_exposure_secondary')

# outcomes
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_outcomes_primary')
spark.sql(f'REFRESH TABLE {dsa}.{proj}_out_outcomes_secondary')
out_primary = spark.table(f'{dsa}.{proj}_out_outcomes_primary')
out_secondary = spark.table(f'{dsa}.{proj}_out_outcomes_secondary')

# reference
person_id_type_lkp = spark.table(path_ref_person_id_type_lkp)

# COMMAND ----------

# MAGIC %md # 2 Check

# COMMAND ----------

display(cohort.limit(10))
count_var(cohort, 'PERSON_ID')

# COMMAND ----------

display(exp_primary.limit(10))
count_var(exp_primary, 'PERSON_ID')

# COMMAND ----------

display(exp_secondary.limit(10))
count_var(exp_secondary, 'PERSON_ID')

# COMMAND ----------

display(out_primary.limit(10))
count_var(out_primary, 'PERSON_ID')

# COMMAND ----------

display(out_secondary.limit(10))
count_var(out_secondary, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

# merge cohort to primary exposure flags
tmp1 = merge(cohort, exp_primary, ['PERSON_ID'], validate='1:1', assert_results=['left_only', 'both'], indicator=0); print()

# COMMAND ----------

# merge cohort to secondary exposure flags
tmp2 = merge(tmp1, exp_secondary, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

# merge cohort to primary outcome flags
tmp3 = merge(tmp2, out_primary, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

# merge cohort to secondary outcome flags
tmp4 = merge(tmp3, out_secondary, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0);

# COMMAND ----------

# add the valid_nhs_number flag from the person_id lookup table for sensitivity analysis
tmp5 = merge(tmp4, person_id_type_lkp.withColumnRenamed('pseudo_id', 'PERSON_ID').select('PERSON_ID', 'valid_nhs_number'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], keep_results = ['both'], indicator=0);

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check
count_var(tmp5, 'PERSON_ID'); print()
print(len(tmp5.columns)); print()
print(pd.DataFrame({f'_cols': tmp5.columns}).to_string()); print()

# COMMAND ----------

display(tmp5)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save final combined table and store previous version
save_table(df = tmp5, out_name = f'{proj}_out_cohort_combined', save_previous=True)

# COMMAND ----------

# save name
#outName = f'{proj}_out_cohort_combined'.lower()

# save previous version for comparison purposes
#tmpt = spark.sql(f"""SHOW TABLES FROM {dsa}""")\
#  .select('tableName')\
#  .where(f.col('tableName') == outName)\
#  .collect()
#if(len(tmpt)>0):
#  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#  print(outName_pre)
#  spark.table(f'{dsa}.{outName}').write.mode('overwrite').saveAsTable(f'{dsa}.{outName_pre}')
#  #spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dsa}')

# save
#spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_03_out_cohort_combined')
#tmp5.write.mode('overwrite').saveAsTable(f'{dsa}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dsa}')

# COMMAND ----------

count_var(tmp5, 'PERSON_ID')