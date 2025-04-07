#
# Derive analysis dataset for heart failure analysis
#
# K. Fleetwood
# 25 Nov 2024: Updated to include people under 40 and over 100
#

#
# 1. Set-up -------------------------------------------------------------------
#

library(tidyverse)
library(DBI)
library(dbplyr)
library(dplyr)
library(janitor)

processed_data_folder <- "~/collab/CCU046/R Studio Server/CCU046_03"

## connect to databricks
con <- 
  dbConnect(
    odbc::odbc(),
    dsn = 'databricks',
    HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
    PWD = rstudioapi::askForPassword('Please enter Databricks PAT') # Use home/token for r
  )

## connect to the cohort table
cohort_tbl <- 
  tbl(
    con, 
    in_schema(
      "dsa_391419_j3w9t_collab", 
      "ccu046_03_out_cohort_combined")
  )

#
# 2. Derive analysis dataset --------------------------------------------------
#

# Determine maximum age in the cohort
# - take all people in the cohort
# - if alive at the end of follow-up, calculate age at end of follow-up
# - if dead during follow-up, calculate age at death
# - calculate maximum
cohort_tbl %>%
  mutate(
    age_at_hf  = DATEDIFF(sql("DAY"), date_of_birth, out_heart_failure_primary_date),
    age_at_hf = age_at_hf/365.25
  ) %>%
  # Overall filter to define the cohort
  filter(
    in_gdppr %in% 1,
    is.na(date_of_death)|(date_of_death > as.Date("2019-11-01")),
    date_of_birth <= as.Date("2023-12-31"),
    is.na(out_heart_failure_primary_date)|(out_heart_failure_primary_date >= as.Date("2019-11-01")),
    is.na(date_of_death)|is.na(out_heart_failure_primary_date)|(date_of_death >= out_heart_failure_primary_date),
  ) %>%
  mutate(
    age_end_fu = DATEDIFF(sql("DAY"), date_of_birth, as.Date("2023-12-31")),
    age_end_fu = age_end_fu/365.25,
    age_at_death = DATEDIFF(sql("DAY"), date_of_birth, date_of_death),
    age_at_death = age_at_death/365.25,
    max_age = 
      case_when(
        is.na(out_heart_failure_primary_date) & is.na(date_of_death) ~ age_end_fu,
        !is.na(out_heart_failure_primary_date) ~ age_at_hf,
        !is.na(date_of_death) ~ age_at_death
      )
  ) %>%
  summarise(
    miss_age_check = sum(as.numeric(is.na(max_age))),
    max_age_cohort = max(max_age)
  )

# Analysis dataset will have one row for each combination of:
# - Month: Nov 2019 to December 2023 (50 months)
# - Age: 0 to (maximum age)
# - Sex: M/F (2 sexes)
# - Deprivation quintile (1:5, missing) (6 levels)
# - Ethnicity (5 groups, missing) (6 levels)
# - SMI group (4 groups)

# For each month, identify people
# - excluding people who died before the start of the month 
# - or who have a previous record of the outcome

# Calculate
# - person-years contributed by each person with
#   - start date: latest of first day of the month, or date of birth
#     (note: don't need to worry about leap birthdays, because all dobs 
#            anonymised to the first of the month)
#   - end date: earliest of outcome event, death or last day of the month

# Summarise by age, sex, deprivation, ethnicity and SMI group, 
# - if the person has their first admission for their most severe SMI within the month, 
#   we will allocate the person years prior to this admission to the non-exposed group and the person-years from this admission to the appropriate SMI group

month_start_ndx <- as.character(seq(as.Date("2019-11-01"), as.Date("2023-12-01"), by='1 month'))
month_end_ndx <- as.character(seq(as.Date("2019-12-01"), as.Date("2024-01-01"), by='1 month') - 1)

month_sum <- 
  data.frame(
    "month_id" = NULL,
    "month_start" = NULL,
    "age" = NULL,
    "sex" = NULL,
    "imd_quintile_lab" = NULL,
    "ethnic_5" = NULL,
    "py_no_smi" = NULL, 
    "py_sch" = NULL,  
    "py_bd" = NULL,  
    "py_dep" = NULL, 
    "events_no_smi" = NULL, 
    "events_sch" = NULL, 
    "events_bd" = NULL, 
    "events_dep" = NULL
  )

for (i in 1:50){
  
  month_start_i <- month_start_ndx[i]
  month_end_i <- month_end_ndx[i]
  
  start_time <- Sys.time()
  month_i_sum <- 
    cohort_tbl %>%
    mutate(
      month_start = as.Date(month_start_i),
      month_end = as.Date(month_end_i),
      age_at_hf  = DATEDIFF(sql("DAY"), date_of_birth, out_heart_failure_primary_date),
      age_at_hf = age_at_hf/365.25,
      age_at_end_month = DATEDIFF(sql("DAY"), date_of_birth, month_end),
      age_at_end_month = age_at_end_month/365.25,
      age_at_end_month_floor = floor(age_at_end_month)
    ) %>%
    # Overall filter to define the cohort
    filter(
      in_gdppr %in% 1,
      is.na(date_of_death)|(date_of_death > as.Date("2019-11-01")),
      date_of_birth <= as.Date("2023-12-31"),
      is.na(out_heart_failure_primary_date)|(out_heart_failure_primary_date >= as.Date("2019-11-01")),
      is.na(date_of_death)|is.na(out_heart_failure_primary_date)|(date_of_death >= out_heart_failure_primary_date)
    ) %>%
    # Specific filter for the index month
    filter(
      is.na(date_of_death)|(date_of_death >= month_start_i),
      is.na(out_heart_failure_primary_date)|(out_heart_failure_primary_date >= month_start_i),
      date_of_birth <= month_end
    ) %>%
    mutate(
      # Define variables
      sch_date_hf = 
        case_when(
          is.na(out_heart_failure_primary_date) ~ primary_exp_schizophrenia_date,
          is.na(primary_exp_schizophrenia_date) ~ NA,
          primary_exp_schizophrenia_date < out_heart_failure_primary_date ~ primary_exp_schizophrenia_date,
          primary_exp_schizophrenia_date >= out_heart_failure_primary_date ~ NA
        ),
      bd_date_hf = 
        case_when(
          is.na(out_heart_failure_primary_date) ~ primary_exp_bipolar_disorder_date,
          is.na(primary_exp_bipolar_disorder_date) ~ NA,
          primary_exp_bipolar_disorder_date < out_heart_failure_primary_date ~ primary_exp_bipolar_disorder_date,
          primary_exp_bipolar_disorder_date >= out_heart_failure_primary_date ~ NA
        ),
      dep_date_hf = 
        case_when(
          is.na(out_heart_failure_primary_date) ~ primary_exp_depression_date,
          is.na(primary_exp_depression_date) ~ NA,
          primary_exp_depression_date < out_heart_failure_primary_date ~ primary_exp_depression_date,
          primary_exp_depression_date >= out_heart_failure_primary_date ~ NA
        ),
      smi_hf = 
        case_when(
          !is.na(sch_date_hf) ~ "Schizophrenia",
          !is.na(bd_date_hf) ~ "Bipolar disorder",
          !is.na(dep_date_hf) ~ "Depression",
          TRUE ~ "No SMI"
        ), 
      imd_quintile_lab = 
        case_when(
          imd_quintile %in% 1 ~ "1 (most deprived)",
          imd_quintile %in% 5 ~ "5 (least deprived)",
          is.na(imd_quintile) ~ "Missing",
          TRUE ~ as.character(imd_quintile)
        ),
      ethnic_5 = 
        case_when(
          ethnicity_19_group %in% c("British", "Irish", "Traveller", "Any other White background") ~ "White",
          ethnicity_19_group %in% c("White and Asian", "White and Black African", "White and Black Caribbean", "Any other mixed background") ~ "Mixed",
          ethnicity_19_group %in% c("Bangladeshi", "Indian", "Pakistani") ~ "South Asian",
          ethnicity_19_group %in% c("African", "Caribbean", "Any other Black background") ~ "Black",
          ethnicity_19_group %in% c("Arab", "Chinese", "Any other Asian background", "Any other ethnic group") ~ "Any other ethnic group",
          is.na(ethnicity_19_group) ~ "Missing"
        )
    ) %>%
    mutate(
      # Calculate person years - need to be careful where someone moves from the non-exposed to exposed group
      #   - start date: latest of first day of the month, or date of birth
      #   - end date: earliest of outcome event, death or last day of the month
      #   - note dates of birth are anonymised to the first of the month
      person_years_start = pmax(month_start, date_of_birth),
      person_years_end = pmin(month_end, date_of_death, out_heart_failure_primary_date),
      person_years_total = DATEDIFF(sql("DAY"), person_years_start, person_years_end) + 1,
      smi_hf_date = 
        case_when(
          smi_hf %in% "No SMI" ~ NA,
          smi_hf %in% "Schizophrenia"~ sch_date_hf,
          smi_hf %in% "Bipolar disorder" ~ bd_date_hf,
          smi_hf %in% "Depression" ~ dep_date_hf
        ),
      smi_start = pmax(month_start, date_of_birth, smi_hf_date),
      person_years_smi = 
        case_when(
          smi_start > month_end  ~ 0,
          smi_start <= month_end ~ DATEDIFF(sql("DAY"), smi_start, person_years_end) + 1
        ),
      person_years_no_smi = 
        case_when(
          smi_hf %in% "No SMI" ~ person_years_total,
          TRUE ~ person_years_total - person_years_smi
        ),
      person_years_sch = 
        case_when(
          smi_hf %in% "Schizophrenia" ~ person_years_smi,
          TRUE ~ 0
        ),
      person_years_bd = 
        case_when(
          smi_hf %in% "Bipolar disorder" ~ person_years_smi,
          TRUE ~ 0
        ),
      person_years_dep = 
        case_when(
          smi_hf %in% "Depression" ~ person_years_smi,
          TRUE ~ 0
        ),
      person_years_total = person_years_total/365.25,
      person_years_no_smi = person_years_no_smi/365.25,
      person_years_sch = person_years_sch/365.25,
      person_years_bd = person_years_bd/365.25,
      person_years_dep = person_years_dep/365.25,
      # Identify events - need to be careful where someone moves from the non-exposed to exposed group
      events_total = 
        case_when(
          !is.na(out_heart_failure_primary_date) & out_heart_failure_primary_date <= month_end & out_heart_failure_primary_date >= date_of_birth ~ 1,
          TRUE                                                             ~ 0
        ),
      events_smi = 
        case_when(
          !is.na(out_heart_failure_primary_date) & out_heart_failure_primary_date <= month_end & out_heart_failure_primary_date >= smi_hf_date & out_heart_failure_primary_date >= date_of_birth ~ 1,
          TRUE                                                                                                   ~ 0
        ),
      events_no_smi = 
        case_when(
          smi_hf %in% "No SMI" ~ events_total,
          TRUE                 ~ events_total - events_smi
        ),
      events_sch = 
        case_when(
          smi_hf %in% "Schizophrenia" ~ events_smi,
          TRUE ~ 0
        ),
      events_bd = 
        case_when(
          smi_hf %in% "Bipolar disorder" ~ events_smi,
          TRUE ~ 0
        ),
      events_dep = 
        case_when(
          smi_hf %in% "Depression" ~ events_smi,
          TRUE ~ 0
        )
    ) %>%
    group_by(
      age_at_end_month_floor, sex, imd_quintile_lab, ethnic_5
    ) %>%
    summarise(
      py_no_smi     = sum(person_years_no_smi),
      py_sch        = sum(person_years_sch),
      py_bd         = sum(person_years_bd),
      py_dep        = sum(person_years_dep),
      events_no_smi = sum(events_no_smi),
      events_sch    = sum(events_sch),
      events_bd     = sum(events_bd),
      events_dep    = sum(events_dep)
    ) %>%
    collect()
  end_time <- Sys.time()
  end_time - start_time
  
  month_i_sum <- 
    month_i_sum %>%
    mutate(
      month_id = i,
      month_start = month_start_i
    ) %>%
    rename(
      age = age_at_end_month_floor
    ) %>%
    dplyr::select(
      month_id, month_start, age, sex, imd_quintile_lab,  ethnic_5,
      py_no_smi, py_sch,  py_bd,  py_dep,
      events_no_smi, events_sch, events_bd, events_dep
    )
  
  month_sum <- bind_rows(month_sum, month_i_sum)
  
}  

#
# 3. Output analysis dataset ---------------------------------------------------
#

# Pivot longer
month_sum_long <- 
  month_sum %>%
  pivot_longer(
    c(starts_with("py"),starts_with("events")),
    names_to = c(".value", "smi"),
    names_pattern = "([[:alpha:]]+)_([[:alpha:]]+)"
  )

saveRDS(month_sum_long, file = file.path(processed_data_folder, "heart_failure_dataset.rds"))  
