#
# Poisson models for myocardial infarction primary analysis
#
# K. Fleetwood
# 8 Aug 2024: date started
#

#
# 1. Set-up -------------------------------------------------------------------
#

library(tidyverse)
library(janitor)
library(DBI)
library(dbplyr)
library(dplyr)
library(MASS) # for confint.glm
library(performance) # for check_overdispersion
library(multcomp)

processed_data_folder <- 
  "D:\\PhotonUser\\My Files\\Home Folder\\CCU046_03\\processed_data"
output_folder <- 
  "D:\\PhotonUser\\My Files\\Home Folder\\CCU046_03\\output"

#
# 2. Load data ----------------------------------------------------------------
#

# Read in mi analysis dataset
mi_data <- readRDS(file = file.path(processed_data_folder, "mi_dataset.rds"))

# Restricted to people aged 40 to 100
mi_data <- 
  mi_data %>%
  filter(age >= 40 & age <= 100)

#
# 3. Modelling ----------------------------------------------------------------
#

#
# 3.1 Principal model (excludes ethnicity) ------------------------------------
#

# 3.1.0 Overall ---------------------------------------------------------------

# 3.1.0.0 Prepare data --------------------------------------------------------

# First subset of mi_data
# Exclude rows with missing deprivation (refactor deprivation)
mi_data_sub1 <-
  mi_data %>%
  filter(
    !imd_quintile_lab %in% "Missing",
    !py == 0
  ) %>%
  mutate(
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
                   "Nov 2023 -\nDec 2023")
        )
  ) %>%
  group_by(
    period_4, age, sex, imd_quintile_lab, smi
  ) %>%
  summarise(
    py = sum(py),
    events = sum(events)
  ) %>%
  mutate(
    imd_quintile_lab = factor(imd_quintile_lab, levels = c("5 (least deprived)", "1 (most deprived)", "2", "3", "4")),
    smi = factor(smi, levels = c("no", "sch", "bd", "dep")),
    age_scale = age - 70
  )

# 3.1.0.1 Model without interaction -------------------------------------------

mi_mod_1a <- 
  glm(
    events ~ age_scale + I(age_scale^2) + sex + imd_quintile_lab + period_4 + smi,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1
  )

check_overdispersion(mi_mod_1a) 

anova(mi_mod_1a, test = "F")

mi_mod_1a_coef <- summary(mi_mod_1a)$coef
mi_mod_1a_ci <- confint(mi_mod_1a)

mi_mod_1a_coef <- cbind(mi_mod_1a_coef, mi_mod_1a_ci)
mi_mod_1a_coef <-
  mi_mod_1a_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  )
head(mi_mod_1a_coef)

# 3.1.0.2 Model with interaction ----------------------------------------------

# Examine effect of adding interaction between SMI and time
mi_mod_1b <- 
  glm(
    events ~ age_scale + I(age_scale^2) + sex + imd_quintile_lab + period_4 + smi + smi:period_4,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1
  )
anova(mi_mod_1a, mi_mod_1b, test = "F")

mi_mod_1b_coef <- summary(mi_mod_1b)$coef
mi_mod_1b_ci <- confint(mi_mod_1b)

mi_mod_1b_coef <- cbind(mi_mod_1b_coef, mi_mod_1b_ci)
mi_mod_1b_coef <-
  mi_mod_1b_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  ) %>%
  mutate(
    p_flag = `Pr(>|t|)` < 0.05
  )


# 3.1.0.3 Estimate SMI IRR for each period ------------------------------------

coefeq <- 
  matrix(
    data = 0,
    nrow = 39, 
    ncol = length(mi_mod_1b$coef)
  )
colnames(coefeq) <- names(mi_mod_1b$coef)

# Fill in main results
coefeq[ 1:13, "smisch"] <- 1
coefeq[14:26, "smibd" ] <- 1
coefeq[27:39, "smidep"] <- 1

# Schizophrenia
coefeq[ 2, "period_4Mar 2020 -\nJun 2020:smisch"] <- 1
coefeq[ 3, "period_4Jul 2020 -\nOct 2020:smisch"] <- 1
coefeq[ 4, "period_4Nov 2020 -\nFeb 2021:smisch"] <- 1
coefeq[ 5, "period_4Mar 2021 -\nJun 2021:smisch"] <- 1
coefeq[ 6, "period_4Jul 2021 -\nOct 2021:smisch"] <- 1
coefeq[ 7, "period_4Nov 2021 -\nFeb 2022:smisch"] <- 1
coefeq[ 8, "period_4Mar 2022 -\nJun 2022:smisch"] <- 1
coefeq[ 9, "period_4Jul 2022 -\nOct 2022:smisch"] <- 1
coefeq[10, "period_4Nov 2022 -\nFeb 2023:smisch"] <- 1
coefeq[11, "period_4Mar 2023 -\nJun 2023:smisch"] <- 1
coefeq[12, "period_4Jul 2023 -\nOct 2023:smisch"] <- 1
coefeq[13, "period_4Nov 2023 -\nDec 2023:smisch"] <- 1

# Bipolar disorder
coefeq[15, "period_4Mar 2020 -\nJun 2020:smibd"] <- 1
coefeq[16, "period_4Jul 2020 -\nOct 2020:smibd"] <- 1
coefeq[17, "period_4Nov 2020 -\nFeb 2021:smibd"] <- 1
coefeq[18, "period_4Mar 2021 -\nJun 2021:smibd"] <- 1
coefeq[19, "period_4Jul 2021 -\nOct 2021:smibd"] <- 1
coefeq[20, "period_4Nov 2021 -\nFeb 2022:smibd"] <- 1
coefeq[21, "period_4Mar 2022 -\nJun 2022:smibd"] <- 1
coefeq[22, "period_4Jul 2022 -\nOct 2022:smibd"] <- 1
coefeq[23, "period_4Nov 2022 -\nFeb 2023:smibd"] <- 1
coefeq[24, "period_4Mar 2023 -\nJun 2023:smibd"] <- 1
coefeq[25, "period_4Jul 2023 -\nOct 2023:smibd"] <- 1
coefeq[26, "period_4Nov 2023 -\nDec 2023:smibd"] <- 1

# Depression
coefeq[28, "period_4Mar 2020 -\nJun 2020:smidep"] <- 1
coefeq[29, "period_4Jul 2020 -\nOct 2020:smidep"] <- 1
coefeq[30, "period_4Nov 2020 -\nFeb 2021:smidep"] <- 1
coefeq[31, "period_4Mar 2021 -\nJun 2021:smidep"] <- 1
coefeq[32, "period_4Jul 2021 -\nOct 2021:smidep"] <- 1
coefeq[33, "period_4Nov 2021 -\nFeb 2022:smidep"] <- 1
coefeq[34, "period_4Mar 2022 -\nJun 2022:smidep"] <- 1
coefeq[35, "period_4Jul 2022 -\nOct 2022:smidep"] <- 1
coefeq[36, "period_4Nov 2022 -\nFeb 2023:smidep"] <- 1
coefeq[37, "period_4Mar 2023 -\nJun 2023:smidep"] <- 1
coefeq[38, "period_4Jul 2023 -\nOct 2023:smidep"] <- 1
coefeq[39, "period_4Nov 2023 -\nDec 2023:smidep"] <- 1

mi_mod_1b_smi_period <- 
  glht(
    mi_mod_1b,
    linfct = coefeq
  )

mi_mod_1b_smi_period_ci <- 
  confint(
    mi_mod_1b_smi_period,
    calpha = univariate_calpha()
  )

mi_mod_1b_smi_period_ci_out <-
  as.data.frame(mi_mod_1b_smi_period_ci$confint) %>%
  mutate(
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression"), each = 13),
    period_4 = rep(levels(mi_data_sub1$period_4), times = 3),
    irr = exp(Estimate),
    irr_low = exp(lwr),
    irr_upp = exp(upr),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(smi, period_4, starts_with("irr"))

# 3.1.1 Females ---------------------------------------------------------------

# 3.1.1.1 Model without interaction -------------------------------------------

mi_mod_1a_f <- 
  glm(
    events ~ age_scale + I(age_scale^2) + imd_quintile_lab + period_4 + smi,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1,
    subset = sex == "F"
  )

check_overdispersion(mi_mod_1a_f)

anova(mi_mod_1a_f, test = "F")

mi_mod_1a_f_coef <- summary(mi_mod_1a_f)$coef
mi_mod_1a_f_ci <- confint(mi_mod_1a_f)

mi_mod_1a_f_coef <- cbind(mi_mod_1a_f_coef, mi_mod_1a_f_ci)
mi_mod_1a_f_coef <-
  mi_mod_1a_f_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  )
mi_mod_1a_f_coef

# 3.1.1.2 Model with interaction ----------------------------------------------

# Examine effect of adding interaction between SMI and time
mi_mod_1b_f <- 
  glm(
    events ~ age_scale + I(age_scale^2) + imd_quintile_lab + period_4 + smi + smi:period_4,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1,
    subset = sex == "F"
  )
anova(mi_mod_1a_f, mi_mod_1b_f, test = "F") 

mi_mod_1b_f_coef <- summary(mi_mod_1b_f)$coef
mi_mod_1b_f_ci <- confint(mi_mod_1b_f)

mi_mod_1b_f_coef <- cbind(mi_mod_1b_f_coef, mi_mod_1b_f_ci)
mi_mod_1b_f_coef <-
  mi_mod_1b_f_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  ) %>%
  mutate(
    p_flag = `Pr(>|t|)` < 0.05
  )


# 3.1.1.3 Estimate SMI IRR for each period ------------------------------------

coefeq_sex <- 
  coefeq[, !colnames(coefeq) %in% "sexM"]

mi_mod_1b_f_smi_period <- 
  glht(
    mi_mod_1b_f,
    linfct = coefeq_sex
  )

mi_mod_1b_f_smi_period_ci <- 
  confint(
    mi_mod_1b_f_smi_period,
    calpha = univariate_calpha()
  )

mi_mod_1b_f_smi_period_ci <-
  as.data.frame(mi_mod_1b_f_smi_period_ci$confint) %>%
  mutate(
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression"), each = 13),
    period_4 = rep(levels(mi_data_sub1$period_4), times = 3),
    irr = exp(Estimate),
    irr_low = exp(lwr),
    irr_upp = exp(upr),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(smi, period_4, starts_with("irr"))

# 3.1.2 Males -----------------------------------------------------------------

# 3.1.2.1 Model without interaction -------------------------------------------

mi_mod_1a_m <- 
  glm(
    events ~ age_scale + I(age_scale^2) + imd_quintile_lab + period_4 + smi,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1,
    subset = sex == "M"
  )

check_overdispersion(mi_mod_1a_m)

anova(mi_mod_1a_m, test = "F")

mi_mod_1a_m_coef <- summary(mi_mod_1a_m)$coef
mi_mod_1a_m_ci <- confint(mi_mod_1a_m)

mi_mod_1a_m_coef <- cbind(mi_mod_1a_m_coef, mi_mod_1a_m_ci)
mi_mod_1a_m_coef <-
  mi_mod_1a_m_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  )

# 3.1.2.2 Model with interaction ----------------------------------------------

# Examine effect of adding interaction between SMI and time
mi_mod_1b_m <- 
  glm(
    events ~ age_scale + I(age_scale^2) + imd_quintile_lab + period_4 + smi + smi:period_4,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub1,
    subset = sex == "M"
  )
anova(mi_mod_1a_m, mi_mod_1b_m, test = "F") 

mi_mod_1b_m_coef <- summary(mi_mod_1b_m)$coef
mi_mod_1b_m_ci <- confint(mi_mod_1b_m)

mi_mod_1b_m_coef <- cbind(mi_mod_1b_m_coef, mi_mod_1b_m_ci)
mi_mod_1b_m_coef <-
  mi_mod_1b_m_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  ) %>%
  mutate(
    p_flag = `Pr(>|t|)` < 0.05
  )

# 3.1.2.3 Estimate SMI IRR for each period ------------------------------------

mi_mod_1b_m_smi_period <- 
  glht(
    mi_mod_1b_m,
    linfct = coefeq_sex
  )

mi_mod_1b_m_smi_period_ci <- 
  confint(
    mi_mod_1b_m_smi_period,
    calpha = univariate_calpha()
  )

mi_mod_1b_m_smi_period_ci <-
  as.data.frame(mi_mod_1b_m_smi_period_ci$confint) %>%
  mutate(
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression"), each = 13),
    period_4 = rep(levels(mi_data_sub1$period_4), times = 3),
    irr = exp(Estimate),
    irr_low = exp(lwr),
    irr_upp = exp(upr),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(smi, period_4, starts_with("irr"))

#
# 3.2 Sensitivity analysis (including ethnicity) ------------------------------
#

# 3.2.0 Overall ---------------------------------------------------------------

# 3.2.0.0 Prepare data --------------------------------------------------------

# - Second subset of mi_data
# - Exclude rows with missing deprivation or missing ethnicity 
#   (refactor deprivation and ethnicity)

mi_data_sub2 <-
  mi_data %>%
  filter(
    !imd_quintile_lab %in% "Missing",
    !ethnic_5 %in% "Missing",
    !py == 0
  ) %>%
  mutate(
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
                   "Nov 2023 -\nDec 2023")
      )
  ) %>%
  group_by(
    period_4, age, sex, imd_quintile_lab, smi, ethnic_5
  ) %>%
  summarise(
    py = sum(py),
    events = sum(events)
  ) %>%
  mutate(
    imd_quintile_lab = factor(imd_quintile_lab, levels = c("5 (least deprived)", "1 (most deprived)", "2", "3", "4")),
    smi = factor(smi, levels = c("no", "sch", "bd", "dep")),
    age_scale = age - 70,
    ethnic_5 = factor(ethnic_5, levels = c("White", "Black", "Mixed", "South Asian", "Any other ethnic group"))
  )

# 3.2.0.1 Model without interaction -------------------------------------------

mi_mod_2a <- 
  glm(
    events ~ age_scale + I(age_scale^2) + sex + imd_quintile_lab + period_4 + smi + ethnic_5,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub2
  )

check_overdispersion(mi_mod_2a)

anova(mi_mod_2a, test = "F")

mi_mod_2a_coef <- summary(mi_mod_2a)$coef
mi_mod_2a_ci <- confint(mi_mod_2a)

mi_mod_2a_coef <- cbind(mi_mod_2a_coef, mi_mod_2a_ci)
mi_mod_2a_coef <-
  mi_mod_2a_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  )
mi_mod_2a_coef

# 3.2.0.2 Model with interaction ----------------------------------------------

# Examine effect of adding interaction between SMI and time
mi_mod_2b <- 
  glm(
    events ~ age_scale + I(age_scale^2) + sex + imd_quintile_lab + period_4 + smi + ethnic_5 + smi:period_4,
    offset = log(py),
    family = quasipoisson, 
    data = mi_data_sub2
  )
anova(mi_mod_2a, mi_mod_2b, test = "F") 

mi_mod_2b_coef <- summary(mi_mod_2b)$coef
mi_mod_2b_ci <- confint(mi_mod_2b)

mi_mod_2b_coef <- cbind(mi_mod_2b_coef, mi_mod_2b_ci)
mi_mod_2b_coef <-
  mi_mod_2b_coef %>%
  as.data.frame() %>%
  mutate(
    irr = exp(Estimate),
    irr_low = exp(`2.5 %`),
    irr_upp = exp(`97.5 %`),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(
    irr, irr_low, irr_upp, irr_fmt, `Pr(>|t|)`
  ) %>%
  mutate(
    p_flag = `Pr(>|t|)` < 0.05
  )

# 3.2.0.3 Estimate SMI IRR for each period ------------------------------------

coefeq <- 
  matrix(
    data = 0,
    nrow = 39, 
    ncol = nrow(mi_mod_2b_coef)
  )
colnames(coefeq) <- row.names(mi_mod_2b_coef)

# Fill in main results
coefeq[ 1:13, "smisch"] <- 1
coefeq[14:26, "smibd" ] <- 1
coefeq[27:39, "smidep"] <- 1

# Schizophrenia
coefeq[ 2, "period_4Mar 2020 -\nJun 2020:smisch"] <- 1
coefeq[ 3, "period_4Jul 2020 -\nOct 2020:smisch"] <- 1
coefeq[ 4, "period_4Nov 2020 -\nFeb 2021:smisch"] <- 1
coefeq[ 5, "period_4Mar 2021 -\nJun 2021:smisch"] <- 1
coefeq[ 6, "period_4Jul 2021 -\nOct 2021:smisch"] <- 1
coefeq[ 7, "period_4Nov 2021 -\nFeb 2022:smisch"] <- 1
coefeq[ 8, "period_4Mar 2022 -\nJun 2022:smisch"] <- 1
coefeq[ 9, "period_4Jul 2022 -\nOct 2022:smisch"] <- 1
coefeq[10, "period_4Nov 2022 -\nFeb 2023:smisch"] <- 1
coefeq[11, "period_4Mar 2023 -\nJun 2023:smisch"] <- 1
coefeq[12, "period_4Jul 2023 -\nOct 2023:smisch"] <- 1
coefeq[13, "period_4Nov 2023 -\nDec 2023:smisch"] <- 1

# Bipolar disorder
coefeq[15, "period_4Mar 2020 -\nJun 2020:smibd"] <- 1
coefeq[16, "period_4Jul 2020 -\nOct 2020:smibd"] <- 1
coefeq[17, "period_4Nov 2020 -\nFeb 2021:smibd"] <- 1
coefeq[18, "period_4Mar 2021 -\nJun 2021:smibd"] <- 1
coefeq[19, "period_4Jul 2021 -\nOct 2021:smibd"] <- 1
coefeq[20, "period_4Nov 2021 -\nFeb 2022:smibd"] <- 1
coefeq[21, "period_4Mar 2022 -\nJun 2022:smibd"] <- 1
coefeq[22, "period_4Jul 2022 -\nOct 2022:smibd"] <- 1
coefeq[23, "period_4Nov 2022 -\nFeb 2023:smibd"] <- 1
coefeq[24, "period_4Mar 2023 -\nJun 2023:smibd"] <- 1
coefeq[25, "period_4Jul 2023 -\nOct 2023:smibd"] <- 1
coefeq[26, "period_4Nov 2023 -\nDec 2023:smibd"] <- 1

# Depression
coefeq[28, "period_4Mar 2020 -\nJun 2020:smidep"] <- 1
coefeq[29, "period_4Jul 2020 -\nOct 2020:smidep"] <- 1
coefeq[30, "period_4Nov 2020 -\nFeb 2021:smidep"] <- 1
coefeq[31, "period_4Mar 2021 -\nJun 2021:smidep"] <- 1
coefeq[32, "period_4Jul 2021 -\nOct 2021:smidep"] <- 1
coefeq[33, "period_4Nov 2021 -\nFeb 2022:smidep"] <- 1
coefeq[34, "period_4Mar 2022 -\nJun 2022:smidep"] <- 1
coefeq[35, "period_4Jul 2022 -\nOct 2022:smidep"] <- 1
coefeq[36, "period_4Nov 2022 -\nFeb 2023:smidep"] <- 1
coefeq[37, "period_4Mar 2023 -\nJun 2023:smidep"] <- 1
coefeq[38, "period_4Jul 2023 -\nOct 2023:smidep"] <- 1
coefeq[39, "period_4Nov 2023 -\nDec 2023:smidep"] <- 1

mi_mod_2b_smi_period <- 
  glht(
    mi_mod_2b,
    linfct = coefeq
  )

mi_mod_2b_smi_period_ci <- 
  confint(
    mi_mod_2b_smi_period,
    calpha = univariate_calpha()
  )

mi_mod_2b_smi_period_ci <-
  as.data.frame(mi_mod_2b_smi_period_ci$confint) %>%
  mutate(
    smi = rep(c("Schizophrenia", "Bipolar disorder", "Depression"), each = 13),
    period_4 = rep(levels(mi_data_sub1$period_4), times = 3),
    irr = exp(Estimate),
    irr_low = exp(lwr),
    irr_upp = exp(upr),
    irr_fmt = 
      paste0(
        format(round(irr, digits = 2), nsmall = 2), " (",
        format(round(irr_low, digits = 2), nsmall = 2), ", ",
        format(round(irr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(smi, period_4, starts_with("irr"))

#
# 4. Output --------------------------------------------------------------------
#

# 4.1 Model 1: overall --------------------------------------------------------

write.csv(
  mi_mod_1a_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1a_coef_",today(),".csv"))
)

write.csv(
  mi_mod_1b_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1b_coef_",today(),".csv"))
)

write.csv(
  mi_mod_1b_smi_period_ci_out,
  file = file.path(output_folder, paste0("04_mi_mod_1b_smi_period_",today(),".csv")),
  row.names = FALSE
)

# 4.2 Model 1: females --------------------------------------------------------

write.csv(
  mi_mod_1a_f_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1a_coef_female",today(),".csv"))
)

write.csv(
  mi_mod_1b_f_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1b_coef_female",today(),".csv"))
)

write.csv(
  mi_mod_1b_f_smi_period_ci,
  file = file.path(output_folder, paste0("04_mi_mod_1b_smi_period_female",today(),".csv")),
  row.names = FALSE
)

# 4.3 Model 1: males ----------------------------------------------------------

write.csv(
  mi_mod_1a_m_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1a_coef_male",today(),".csv"))
)

write.csv(
  mi_mod_1b_m_coef,
  file = file.path(output_folder, paste0("04_mi_mod_1b_coef_male",today(),".csv"))
)

write.csv(
  mi_mod_1b_m_smi_period_ci,
  file = file.path(output_folder, paste0("04_mi_mod_1b_smi_period_male",today(),".csv")),
  row.names = FALSE
)

# 4.4 Model 2: overall --------------------------------------------------------

write.csv(
  mi_mod_2a_coef,
  file = file.path(output_folder, paste0("04_mi_mod_2a_coef_",today(),".csv"))
)

write.csv(
  mi_mod_2b_coef,
  file = file.path(output_folder, paste0("04_mi_mod_2b_coef_",today(),".csv"))
)

write.csv(
  mi_mod_2b_smi_period_ci,
  file = file.path(output_folder, paste0("04_mi_mod_2b_smi_period_",today(),".csv")),
  row.names = FALSE
)
