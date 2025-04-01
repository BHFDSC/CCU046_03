#
# Age-standardised incidence rates for heart failure
#
# K. Fleetwood
# 24 Sep 2024: adapted from equivalent stroke code
#

#
# 1. Set-up -------------------------------------------------------------------
#

library(tidyverse)
library(janitor)
library(DBI)
library(dbplyr)
library(dplyr)
library(epitools) # for ageadjust.direct

processed_data_folder <- "~/collab/CCU046/R Studio Server/CCU046_03"
output_folder <- "~/collab/CCU046/R Studio Server/CCU046_03"

## connect to databricks
con <- 
  dbConnect(
    odbc::odbc(),
    dsn = 'databricks',
    HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
    PWD = rstudioapi::askForPassword('Please enter Databricks PAT') # Use home/token for r
  )

#
# 2. Load data ----------------------------------------------------------------
#

# Read in primary heart failure analysis dataset
hf_data <- readRDS(file = file.path(processed_data_folder, "heart_failure_dataset.rds"))

# Connect to the European standard population
# age-standardised table: hds_european_standard_population_by_sex
esp_tbl <- 
  tbl(
    con, 
    in_schema(
      "dsa_391419_j3w9t_collab", 
      "hds_european_standard_population_by_sex")
  )

esp <- esp_tbl %>% collect()

#
# 3. Process data -------------------------------------------------------------
#

# 3.1 Pre-processing  -------------------------------------------------------

# Define age groups
# Define periods
hf_data <- 
  hf_data %>%
  mutate(
    age_group = 
      case_when(
                    age <  5 ~   "0-4 years",
        age >=  5 & age < 10 ~   "5-9 years", 
        age >= 10 & age < 15 ~ "10-14 years",
        age >= 15 & age < 20 ~ "15-19 years", 
        age >= 20 & age < 25 ~ "20-24 years", 
        age >= 25 & age < 30 ~ "25-29 years", 
        age >= 30 & age < 35 ~ "30-34 years", 
        age >= 35 & age < 40 ~ "35-39 years",         
        age >= 40 & age < 45 ~ "40-44 years",
        age >= 45 & age < 50 ~ "45-49 years", 
        age >= 50 & age < 55 ~ "50-54 years",
        age >= 55 & age < 60 ~ "55-59 years", 
        age >= 60 & age < 65 ~ "60-64 years", 
        age >= 65 & age < 70 ~ "65-69 years", 
        age >= 70 & age < 75 ~ "70-74 years", 
        age >= 75 & age < 80 ~ "75-79 years", 
        age >= 80 & age < 85 ~ "80-84 years",
        age >= 85 & age < 90 ~ "85-89 years", 
        age >= 90            ~ "90plus years"
      ),
    month_start = as.Date(month_start),
    period_4 = 
      case_when(
        month_start >= as.Date("2019-11-01") & month_start <= as.Date("2020-02-29") ~ "Nov 2019 -\nFeb 2020",
        month_start >= as.Date("2020-03-01") & month_start <= as.Date("2020-06-30") ~ "Mar 2020 -\nJun 2020",
        month_start >= as.Date("2020-07-01") & month_start <= as.Date("2020-10-31") ~ "Jul 2020 -\nOct 2020",
        month_start >= as.Date("2020-11-01") & month_start <= as.Date("2021-02-28") ~ "Nov 2020 -\nFeb 2021",
        month_start >= as.Date("2021-03-01") & month_start <= as.Date("2021-06-30") ~ "Mar 2021 -\nJun 2021",
        month_start >= as.Date("2021-07-01") & month_start <= as.Date("2021-10-31") ~ "Jul 2021 -\nOct 2021",
        month_start >= as.Date("2021-11-01") & month_start <= as.Date("2022-02-28") ~ "Nov 2021 -\nFeb 2022",
        month_start >= as.Date("2022-03-01") & month_start <= as.Date("2022-06-30") ~ "Mar 2022 -\nJun 2022",
        month_start >= as.Date("2022-07-01") & month_start <= as.Date("2022-10-31") ~ "Jul 2022 -\nOct 2022",
        month_start >= as.Date("2022-11-01") & month_start <= as.Date("2023-02-28") ~ "Nov 2022 -\nFeb 2023",
        month_start >= as.Date("2023-03-01") & month_start <= as.Date("2023-06-30") ~ "Mar 2023 -\nJun 2023",
        month_start >= as.Date("2023-07-01") & month_start <= as.Date("2023-10-31") ~ "Jul 2023 -\nOct 2023",
        month_start >= as.Date("2023-11-01") & month_start <= as.Date("2023-12-31") ~ "Nov 2023 -\nDec 2023",
      ),
    period_4 = 
      factor(
        period_4, 
        levels = c("Nov 2019 -\nFeb 2020", "Mar 2020 -\nJun 2020", "Jul 2020 -\nOct 2020",
                   "Nov 2020 -\nFeb 2021", "Mar 2021 -\nJun 2021", "Jul 2021 -\nOct 2021",
                   "Nov 2021 -\nFeb 2022", "Mar 2022 -\nJun 2022", "Jul 2022 -\nOct 2022",
                   "Nov 2022 -\nFeb 2023", "Mar 2023 -\nJun 2023", "Jul 2023 -\nOct 2023",
                   "Nov 2023 -\nDec 2023"
        )
      )
  )

# Wrangle ESP
esp <- 
  esp %>% 
  filter(
    age_group %in% pull(hf_data, age_group)
  ) %>%
  mutate(
    sex = substring(sex, 1, 1)
  )

# ESP - all (i.e not stratified by sex)
# ESP (whole popn) is equivalent to ESP for either sex
# (ESP is the same for each sex)
esp_all <- 
  esp %>%
  filter(
    sex %in% "M"
  ) %>%
  select(-sex)

# 3.2 Define data for monthly incidence (not by SMI status) -------------------

hf_data_grp_1 <- 
  hf_data %>%    
  group_by(month_start, age_group, sex) %>%
  summarise(
    tot_py = sum(py),
    tot_events = sum(events)
  )

# Merge in European Standard Population
hf_data_grp_1 <-
  hf_data_grp_1 %>%
  left_join(
    esp
  )

hf_data_grp_1 <- 
  hf_data_grp_1 %>%
  mutate(
    euro_standard_pop = as.numeric(euro_standard_pop)
  )
hf_data_grp_1 %>% tabyl(euro_standard_pop)

# 3.3 Define data for monthly incidence (not stratified by sex) ---------------

hf_data_grp_1_all <- 
  hf_data %>%    
  group_by(month_start, age_group) %>%
  summarise(
    tot_py = sum(py),
    tot_events = sum(events)
  )

# Merge in European Standard Population
hf_data_grp_1_all <-
  hf_data_grp_1_all %>%
  left_join(
    esp_all
  ) %>%
  mutate(
    euro_standard_pop = as.numeric(euro_standard_pop)
  )

# 3.4 Define data for 4-monthly incidence by SMI status -----------------------

hf_data_grp_4 <- 
  hf_data %>%
  filter(age >= 40 & age <= 100) %>%
  group_by(period_4, smi, age_group, sex) %>%
  summarise(
    tot_py = sum(py),
    tot_events = sum(events)
  ) %>%
  pivot_wider(
    names_from = smi, 
    values_from = c(tot_py, tot_events)
  )

hf_data_grp_4 <-
  hf_data_grp_4 %>%
  left_join(
    esp
  ) %>%
  mutate(
    euro_standard_pop = as.numeric(euro_standard_pop)
  )

# 3.5 Define data for 4-monthly incidence (not stratified by sex) -----------------------

hf_data_grp_4_all <- 
  hf_data %>% 
  filter(age >= 40 & age <= 100) %>%
  group_by(period_4, smi, age_group) %>%
  summarise(
    tot_py = sum(py),
    tot_events = sum(events)
  ) %>%
  pivot_wider(
    names_from = smi, 
    values_from = c(tot_py, tot_events)
  )

hf_data_grp_4_all <-
  hf_data_grp_4_all %>%
  left_join(
    esp_all
  ) %>%
  mutate(
    euro_standard_pop = as.numeric(euro_standard_pop)
  )

#
# 4. Calculate age-standardised incidence rates -------------------------------
#

# 4.1 Monthly age-standardised incidence rates (not by SMI status) ------------

# For each month calculate age-standardised incidence rate

month_starts <- unique(hf_data_grp_1$month_start)
n_mths <- length(month_starts)

age_std_1 <- 
  data.frame(
    month_start = rep(month_starts, each = 2),
    sex = rep(c("Male", "Female"), times = n_mths),
    crude.rate = NA,
    adj.rate = NA,
    lci = NA, 
    uci = NA
  )

for (mth in month_starts){
  
  ndx_female <- hf_data_grp_1$month_start == mth & hf_data_grp_1$sex == "F"
  ndx_male   <- hf_data_grp_1$month_start == mth & hf_data_grp_1$sex == "M"
  
  # Age adjusted estimates
  hf_std_f <- 
    ageadjust.direct(
      count = hf_data_grp_1$tot_events[ndx_female],
      pop = hf_data_grp_1$tot_py[ndx_female],
      stdpop = hf_data_grp_1$euro_standard_pop[ndx_female]
    )
  
  hf_std_m <- 
    ageadjust.direct(
      count = hf_data_grp_1$tot_events[ndx_male],
      pop = hf_data_grp_1$tot_py[ndx_male],
      stdpop = hf_data_grp_1$euro_standard_pop[ndx_male]
    )
  
  # Add to data frame
  age_std_1[age_std_1$month_start == mth & age_std_1$sex == "Female", 3:6] <- hf_std_f
  age_std_1[age_std_1$month_start == mth & age_std_1$sex == "Male", 3:6]   <- hf_std_m
  
}

# Multiply by 1000, create formatted version
age_std_1 <- 
  age_std_1 %>%
  mutate(
    crude.rate = 1000 * crude.rate,
    adj.rate   = 1000 * adj.rate, 
    lci        = 1000 * lci,
    uci        = 1000 * uci,
    adj.fmt    = 
      paste0(
        format(round(adj.rate, digits = 2), nsmall = 2), " (", 
        format(round(lci, digits = 2), nsmall = 2), ", ", 
        format(round(uci, digits = 2), nsmall = 2), ")"
      )
  )

age_std_1 %>%
  group_by(sex) %>%
  summarise(
    adj.rate = mean(adj.rate)
  )

# Plot age-standardised incidence rates
ggplot(age_std_1, aes(x = month_start, y = adj.rate, color = sex, fill = sex)) + 
  geom_line() +
  geom_ribbon(aes(ymin = lci, ymax = uci), alpha=0.1) + 
  theme_bw() + 
  labs(
    x = "Year", 
    y = "Heart failure: age standardised incidence rate per 1000 person-years", 
    color = "Sex", fill = "Sex"
  )

ggsave(
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_month_plot_",today(),".pdf")),
  width = 10, height = 8
)


# 4.2 Monthly age-standardised incidence rates (not stratified by sex) ------------

# For each month calculate age-standardised incidence rate

age_std_1_all <- 
  data.frame(
    month_start = month_starts,
    crude.rate = NA,
    adj.rate = NA,
    lci = NA, 
    uci = NA
  )

for (mth in month_starts){
  
  ndx <- hf_data_grp_1_all$month_start == mth
  
  # Age adjusted estimates
  hf_std <- 
    ageadjust.direct(
      count = hf_data_grp_1_all$tot_events[ndx],
      pop = hf_data_grp_1_all$tot_py[ndx],
      stdpop = hf_data_grp_1_all$euro_standard_pop[ndx]
    )
  
  # Add to data frame
  age_std_1_all[age_std_1_all$month_start == mth, 2:5] <- hf_std
  
}


# Multiply by 1000, create formatted version
age_std_1_all <- 
  age_std_1_all %>%
  mutate(
    crude.rate = 1000 * crude.rate,
    adj.rate   = 1000 * adj.rate, 
    lci        = 1000 * lci,
    uci        = 1000 * uci,
    adj.fmt    = 
      paste0(
        format(round(adj.rate, digits = 2), nsmall = 2), " (", 
        format(round(lci, digits = 2), nsmall = 2), ", ", 
        format(round(uci, digits = 2), nsmall = 2), ")"
      )
  )


# Plot age-standardised incidence rates
ggplot(age_std_1_all, aes(x = month_start, y = adj.rate)) + 
  geom_line() +
  geom_ribbon(aes(ymin = lci, ymax = uci), alpha=0.1) + 
  theme_bw() + 
  labs(
    x = "Year", 
    y = "Heart failure: age standardised incidence rate per 1000 person-years"
  )

ggsave(
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_month_plot_all_",today(),".pdf")),
  width = 10, height = 8
)

# 4.3 4-monthly age-standardised incidence rates by SMI -----------------------

# For each period, and each condition calculate age-standardised incidence rate

period_4s <- unique(hf_data_grp_4$period_4)
n_pers <- length(period_4s)

age_std_4 <- 
  data.frame(
    period_4 = rep(period_4s, each = 8),
    period_4_no = rep(1:13, each = 8),
    sex = rep(rep(c("Male", "Female"), each = 4), times = n_pers),
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression", "No SMI"), times = 2 * n_pers),
    crude.rate = NA,
    adj.rate = NA,
    lci = NA, 
    uci = NA
  )

for (per in period_4s){
  
  ndx_female <- hf_data_grp_4$period_4 == per & hf_data_grp_4$sex == "F"
  ndx_male   <- hf_data_grp_4$period_4 == per & hf_data_grp_4$sex == "M"
  
  # Age adjusted estimate for schizophrenia
  hf_std_sch_f <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_sch[ndx_female],
      pop = hf_data_grp_4$tot_py_sch[ndx_female],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_female]
    )
  
  hf_std_sch_m <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_sch[ndx_male],
      pop = hf_data_grp_4$tot_py_sch[ndx_male],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_male]
    )
  
  # Age adjusted estimate for BD
  hf_std_bd_f <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_bd[ndx_female],
      pop = hf_data_grp_4$tot_py_bd[ndx_female],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_female]
    )
  
  hf_std_bd_m <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_bd[ndx_male],
      pop = hf_data_grp_4$tot_py_bd[ndx_male],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_male]
    )
  
  # Age adjusted estimate for dep
  hf_std_dep_f <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_dep[ndx_female],
      pop = hf_data_grp_4$tot_py_dep[ndx_female],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_female]
    )
  
  hf_std_dep_m <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_dep[ndx_male],
      pop = hf_data_grp_4$tot_py_dep[ndx_male],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_male]
    )
  
  # Age adjusted estimate for no SMI
  hf_std_no_smi_f <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_no[ndx_female],
      pop = hf_data_grp_4$tot_py_no[ndx_female],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_female]
    )
  
  hf_std_no_smi_m <- 
    ageadjust.direct(
      count = hf_data_grp_4$tot_events_no[ndx_male],
      pop = hf_data_grp_4$tot_py_no[ndx_male],
      stdpop = hf_data_grp_4$euro_standard_pop[ndx_male]
    )
  
  # Add to data frame
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Schizophrenia" & age_std_4$sex == "Female", 5:8] <- hf_std_sch_f
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Schizophrenia" & age_std_4$sex == "Male", 5:8]   <- hf_std_sch_m
  
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Bipolar disorder" & age_std_4$sex == "Female", 5:8] <- hf_std_bd_f
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Bipolar disorder" & age_std_4$sex == "Male", 5:8]   <- hf_std_bd_m
  
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Depression" & age_std_4$sex == "Female", 5:8] <- hf_std_dep_f
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "Depression" & age_std_4$sex == "Male", 5:8]   <- hf_std_dep_m
  
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "No SMI" & age_std_4$sex == "Female", 5:8] <- hf_std_no_smi_f
  age_std_4[age_std_4$period_4 == per & age_std_4$smi == "No SMI" & age_std_4$sex == "Male", 5:8] <- hf_std_no_smi_m
  
}


# Multiply by 1000, create formatted version
age_std_4 <- 
  age_std_4 %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression", "No SMI")),
    crude.rate = 1000 * crude.rate,
    adj.rate   = 1000 * adj.rate, 
    lci        = 1000 * lci,
    uci        = 1000 * uci,
    adj.fmt    = 
      paste0(
        format(round(adj.rate, digits = 2), nsmall = 2), " (", 
        format(round(lci, digits = 2), nsmall = 2), ", ", 
        format(round(uci, digits = 2), nsmall = 2), ")"
      )
  )

age_std_4_no_smi <- 
  age_std_4 %>% 
  filter(smi == "No SMI") %>%
  dplyr::select(-smi)

age_std_4_no_smi_sch <- 
  age_std_4_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

age_std_4_no_smi_bd <- 
  age_std_4_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

age_std_4_no_smi_dep <- 
  age_std_4_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

# Create version of age_std_4 for plotting
age_std_4_plot <- 
  age_std_4 %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    age_std_4_no_smi_sch,
    age_std_4_no_smi_bd,
    age_std_4_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y")
  )

# Plot age-standardised incidence rates
ggplot(age_std_4_plot, aes(x = period_4_date, y = adj.rate, color = smi_status)) + 
  geom_line() +
  geom_ribbon(aes(ymin = lci, ymax = uci, fill = smi_status), alpha = 0.2) + 
  facet_grid(cols = vars(smi), rows = vars(sex)) +
  theme_bw() + 
  theme(
    strip.background = element_rect(fill = "white")
  ) +
  labs(
    x = "Year", 
    y = "Age standardised incidence rate per 1000 person-years",
    color = "", fill = ""
  )

ggsave(
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_smi_plot_",today(),".pdf")),
  width = 10, height = 8
)

# 4.4 4-monthly age-standardised incidence rates by SMI (not stratified by sex) -------

# For each period, and each condition calculate age-standardised incidence rate

age_std_4_all <- 
  data.frame(
    period_4 = rep(period_4s, each = 4),
    period_4_no = rep(1:13, each = 4),
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression", "No SMI"), times = n_pers),
    crude.rate = NA,
    adj.rate = NA,
    lci = NA, 
    uci = NA
  )

for (per in period_4s){
  
  ndx <- hf_data_grp_4_all$period_4 == per
  
  # Age adjusted estimate for schizophrenia
  hf_std_sch <- 
    ageadjust.direct(
      count  = hf_data_grp_4_all$tot_events_sch[ndx],
      pop    = hf_data_grp_4_all$tot_py_sch[ndx],
      stdpop = hf_data_grp_4_all$euro_standard_pop[ndx]
    )
  
  # Age adjusted estimate for BD
  hf_std_bd <- 
    ageadjust.direct(
      count  = hf_data_grp_4_all$tot_events_bd[ndx],
      pop    = hf_data_grp_4_all$tot_py_bd[ndx],
      stdpop = hf_data_grp_4_all$euro_standard_pop[ndx]
    )
  
  # Age adjusted estimate for dep
  hf_std_dep <- 
    ageadjust.direct(
      count  = hf_data_grp_4_all$tot_events_dep[ndx],
      pop    = hf_data_grp_4_all$tot_py_dep[ndx],
      stdpop = hf_data_grp_4_all$euro_standard_pop[ndx]
    )
  
  # Age adjusted estimate for no SMI
  hf_std_no_smi <- 
    ageadjust.direct(
      count  = hf_data_grp_4_all$tot_events_no[ndx],
      pop    = hf_data_grp_4_all$tot_py_no[ndx],
      stdpop = hf_data_grp_4_all$euro_standard_pop[ndx]
    )
  
  # Add to data frame
  age_std_4_all[age_std_4_all$period_4 == per & age_std_4_all$smi == "Schizophrenia", 4:7] <- hf_std_sch
  age_std_4_all[age_std_4_all$period_4 == per & age_std_4_all$smi == "Bipolar disorder", 4:7] <- hf_std_bd
  age_std_4_all[age_std_4_all$period_4 == per & age_std_4_all$smi == "Depression", 4:7] <- hf_std_dep
  age_std_4_all[age_std_4_all$period_4 == per & age_std_4_all$smi == "No SMI", 4:7] <- hf_std_no_smi
  
}


# Multiply by 1000, create formatted version
age_std_4_all <- 
  age_std_4_all %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression", "No SMI")),
    crude.rate = 1000 * crude.rate,
    adj.rate   = 1000 * adj.rate, 
    lci        = 1000 * lci,
    uci        = 1000 * uci,
    adj.fmt    = 
      paste0(
        format(round(adj.rate, digits = 2), nsmall = 2), " (", 
        format(round(lci, digits = 2), nsmall = 2), ", ", 
        format(round(uci, digits = 2), nsmall = 2), ")"
      )
  )

age_std_4_all_no_smi <- 
  age_std_4_all %>% 
  filter(smi == "No SMI") %>%
  dplyr::select(-smi)

age_std_4_all_no_smi_sch <- 
  age_std_4_all_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

age_std_4_all_no_smi_bd <- 
  age_std_4_all_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

age_std_4_all_no_smi_dep <- 
  age_std_4_all_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

# Create version of age_std_4_all for plotting
age_std_4_all_plot <- 
  age_std_4_all %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    age_std_4_all_no_smi_sch,
    age_std_4_all_no_smi_bd,
    age_std_4_all_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y")
  )

# Plot age-standardised incidence rates
ggplot(age_std_4_all_plot, aes(x = period_4_date, y = adj.rate, color = smi_status)) + 
  geom_line() +
  geom_ribbon(aes(ymin = lci, ymax = uci, fill = smi_status), alpha = 0.2) + 
  facet_grid(cols = vars(smi)) +
  theme_bw() + 
  theme(
    strip.background = element_rect(fill = "white")
  ) +
  labs(
    x = "Year", 
    y = "Age standardised incidence rate per 1000 person-years",
    color = "", fill = ""
  )

ggsave(
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_smi_plot_all_",today(),".pdf")),
  width = 10, height = 6
)

#
# 5. Output age-standardised incidence rates ----------------------------------
#

write.csv(
  age_std_1,
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_month_",today(),".csv")),
  row.names = FALSE
)

write.csv(
  age_std_1_all,
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_month_all_",today(),".csv")),
  row.names = FALSE
)

write.csv(
  age_std_4, 
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_smi_",today(),".csv")),
  row.names = FALSE
)

write.csv(
  age_std_4_all, 
  file = file.path(output_folder, paste0("03_heart_failure_age_standardised_smi_all_",today(),".csv")),
  row.names = FALSE
)
