#
# Combined age-standardised incidence plots
#
# K.Fleetwood
# 26 November 2024
# 

#
# 1. Set-up --------------------------------------------------------------------
#

library(tidyverse)
library(lubridate)

incidence_folder <- 
  "U:\\Datastore\\CMVM\\smgphs\\groups\\v1cjack1-CSO-SMI-MI\\Incidence"

res_folder <- file.path(incidence_folder, "Results")
nov_folder <- file.path(incidence_folder, "Results - Nov 2024")
fig_folder <- file.path(incidence_folder, "Figures")


#
# 2. Load data ----------------------------------------------------------------
#

# 2.1 Overall population ------------------------------------------------------

mi <- 
  read.csv(
    file.path(
      nov_folder,
      "04_mi_1_age_standardised_month_all2024-11-25.csv"
    )
  )

strk <- 
  read.csv(
    file.path(
      nov_folder,
      "04_stroke_1_age_standardised_month_all_2024-11-25.csv"
    )
  )

hf <- 
  read.csv(
    file.path(
      nov_folder,
      "04_heart_failure_1_age_standardised_month_all_2024-11-25.csv"
    )
  )

mi <- 
  mi %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Myocardial infarction"
  )

strk <- 
  strk %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Stroke"
  )

hf <- 
  hf %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Heart failure"
  )

cond_lvls <- c("Myocardial infarction", "Heart failure", "Stroke")

as_overall <- 
  bind_rows(
    mi,
    strk,
    hf
  ) %>%
  mutate(
    condition = factor(condition, levels = cond_lvls)
  )

# 2.2 Overall population by sex -----------------------------------------------

mi_sex <- 
  read.csv(
    file.path(
      nov_folder,
      "04_mi_1_age_standardised_month_2024-11-25.csv"
      )
  )

strk_sex <- 
  read.csv(
    file.path(
      nov_folder,
      "04_stroke_1_age_standardised_month_2024-11-25.csv"
    )
  )

hf_sex <- 
  read.csv(
    file.path(
      nov_folder,
      "04_heart_failure_1_age_standardised_month_2024-11-25.csv"
    )
  )

mi_sex <- 
  mi_sex %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Myocardial infarction"
  )

strk_sex <- 
  strk_sex %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Stroke"
  )

hf_sex <- 
  hf_sex %>% 
  mutate(
    month_start = as.Date(month_start),
    condition = "Heart failure"
  )

as_sex <- 
  bind_rows(
    mi_sex,
    strk_sex,
    hf_sex
  ) %>%
  mutate(
    condition = factor(condition, levels = cond_lvls)
  )

# 2.3 Overall population by SMI ---------------------------------------------------

mi_smi <- 
  read.csv(
    file.path(
      nov_folder,
      "04_mi_1_age_standardised_smi_all_2024-11-25.csv"
    )
  )

strk_smi <- 
  read.csv(
    file.path(
      nov_folder,
      "04_stroke_1_age_standardised_smi_all_2024-11-25.csv"
    )
  )

hf_smi <- 
  read.csv(
    file.path(
      nov_folder,
      "04_heart_failure_1_age_standardised_smi_all_2024-11-25.csv"
    )
  )

# Re-arrange MI data for plotting

mi_smi_no_smi <- 
  mi_smi %>%
  filter(smi == "No SMI") %>%
  select(-smi)

mi_smi_no_smi_sch <- 
  mi_smi_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

mi_smi_no_smi_bd <- 
  mi_smi_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

mi_smi_no_smi_dep <- 
  mi_smi_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

mi_smi_plot <- 
  mi_smi %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    mi_smi_no_smi_sch,
    mi_smi_no_smi_bd,
    mi_smi_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Myocardial infarction"
  )

# Re-arrange stroke data for plotting

strk_smi_no_smi <- 
  strk_smi %>%
  filter(smi == "No SMI") %>%
  select(-smi)

strk_smi_no_smi_sch <- 
  strk_smi_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

strk_smi_no_smi_bd <- 
  strk_smi_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

strk_smi_no_smi_dep <- 
  strk_smi_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

strk_smi_plot <- 
  strk_smi %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    strk_smi_no_smi_sch,
    strk_smi_no_smi_bd,
    strk_smi_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Stroke"
  )

# Re-arrange HF data for plotting

hf_smi_no_smi <- 
  hf_smi %>%
  filter(smi == "No SMI") %>%
  select(-smi)

hf_smi_no_smi_sch <- 
  hf_smi_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

hf_smi_no_smi_bd <- 
  hf_smi_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

hf_smi_no_smi_dep <- 
  hf_smi_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

hf_smi_plot <- 
  hf_smi %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    hf_smi_no_smi_sch,
    hf_smi_no_smi_bd,
    hf_smi_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Heart failure"
  )

# Combine across conditions

as_smi <- 
  bind_rows(
    mi_smi_plot ,
    strk_smi_plot ,
    hf_smi_plot
  ) %>%
  mutate(
    condition = factor(condition, levels = cond_lvls)
  )

# 2.4 Overall population by SMI and sex -------------------------------------------

mi_ss <- 
  read.csv(
    file.path(
      res_folder,
      "04_mi_1_age_standardised_smi_2024-10-23.csv"
    )
  )

strk_ss <- 
  read.csv(
    file.path(
      res_folder,
      "04_stroke_1_age_standardised_smi_2024-10-23.csv"
    )
  )

hf_ss <- 
  read.csv(
    file.path(
      res_folder,
      "04_heart_failure_1_age_standardised_smi_2024-10-23.csv"
    )
  )

# Re-arrange MI data for plotting

mi_ss_no_smi <- 
  mi_ss %>%
  filter(smi == "No SMI") %>%
  select(-smi)

mi_ss_no_smi_sch <- 
  mi_ss_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

mi_ss_no_smi_bd <- 
  mi_ss_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

mi_ss_no_smi_dep <- 
  mi_ss_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

mi_ss_plot <- 
  mi_ss %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    mi_ss_no_smi_sch,
    mi_ss_no_smi_bd,
    mi_ss_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Myocardial infarction"
  )

# Re-arrange stroke data for plotting

strk_ss_no_smi <- 
  strk_ss %>%
  filter(smi == "No SMI") %>%
  select(-smi)

strk_ss_no_smi_sch <- 
  strk_ss_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

strk_ss_no_smi_bd <- 
  strk_ss_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

strk_ss_no_smi_dep <- 
  strk_ss_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

strk_ss_plot <- 
  strk_ss %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    strk_ss_no_smi_sch,
    strk_ss_no_smi_bd,
    strk_ss_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Stroke"
  )

# Re-arrange HF data for plotting

hf_ss_no_smi <- 
  hf_ss %>%
  filter(smi == "No SMI") %>%
  select(-smi)

hf_ss_no_smi_sch <- 
  hf_ss_no_smi %>%
  mutate(
    smi = "Schizophrenia",
    smi_status = "No SMI"
  )

hf_ss_no_smi_bd <- 
  hf_ss_no_smi %>%
  mutate(
    smi = "Bipolar disorder",
    smi_status = "No SMI"
  )

hf_ss_no_smi_dep <- 
  hf_ss_no_smi %>%
  mutate(
    smi = "Depression",
    smi_status = "No SMI"
  )

hf_ss_plot <- 
  hf_ss %>%
  filter(
    !smi %in% "No SMI"
  ) %>%
  mutate(
    smi_status = "SMI"
  ) %>%
  bind_rows(
    hf_ss_no_smi_sch,
    hf_ss_no_smi_bd,
    hf_ss_no_smi_dep
  ) %>%
  mutate(
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    smi_status = factor(smi_status, levels = c("SMI", "No SMI")),
    period_4_start = paste("1", substring(period_4, 1, 8)),
    period_4_date = as.Date(period_4_start, format = "%d %b %Y"),
    condition = "Heart failure"
  )



as_ss <- 
  bind_rows(
    mi_ss_plot,
    strk_ss_plot,
    hf_ss_plot
  ) %>%
  mutate(
    condition = factor(condition, levels = cond_lvls)
  )

#
# 3. Combined graphs ----------------------------------------------------------
#

# 3.1 Overall population ------------------------------------------------------

ggplot(
  as_overall, 
  aes(x = month_start, y = adj.rate)) +
  facet_wrap(vars(condition), nrow = 3, scales = "free_y") +
  #facetted_pos_scales(y = scales_all) + 
  geom_line(col="#00BFC4") + 
  geom_ribbon(aes(ymin = lci, ymax = uci), fill="#00BFC4", alpha = 0.2) + 
  theme_bw() + 
  theme(
    text=element_text(size=16),
    strip.background = element_rect(fill = "White"),
    panel.grid = element_blank()
  ) +
  labs(
    x = "Year", 
    y = "Age-standardised incidence rate\n per 1000 person-years"
  )

ggsave(
  file = file.path(fig_folder, "age_standardised_plot_v3.pdf"),
  width = 7, 
  height = 10.5
)

# 3.2 Overall population by sex -----------------------------------------------


ggplot(
  as_sex, 
  aes(x = month_start, y = adj.rate, group = sex, color = sex, fill = sex)) +
  facet_wrap(vars(condition), nrow = 3, scales = "free_y") +
  geom_line() + 
  geom_ribbon(aes(ymin = lci, ymax = uci), colour = NA, alpha = 0.2) + 
  theme_bw() + 
  theme(
    text=element_text(size=20),
    strip.background = element_rect(fill = "White"),
    panel.grid = element_blank()
  ) +
  labs(
    x = "Year", 
    y = "Age-standardised incidence rate\n per 1000 person-years",
    color = "Sex",
    fill = "Sex"
  )

ggsave(
  file = file.path(fig_folder, "age_standardised_sex_plot_v2.pdf"),
  width = 8, 
  height = 10.5
)

# 3.3 Overall population by SMI -----------------------------------------------

ggplot(as_smi, aes(x = period_4_date, y = adj.rate, color = smi_status)) +
  geom_line() + 
  geom_ribbon(aes(ymin = lci, ymax = uci, fill = smi_status), colour = NA, alpha = 0.2) +
  facet_grid(cols = vars(smi), rows = vars(condition), scales = "free_y") + 
  theme_bw() +
  theme(
    strip.background = element_rect(fill = "White"),
    panel.grid = element_blank()
  ) + 
  labs(
    x = "Year", 
    y = "Age-standardised incidence rate per 1000 person-years",
    color = "", fill = ""
  )

ggsave(
  file = file.path(fig_folder, "age_standardised_smi_plot_v2.pdf"),
  width = 10, 
  height = 10
)


# 3.4 Overall population by sex and SMI ---------------------------------------

ggplot(as_ss, aes(x = period_4_date, y = adj.rate, color = smi_status)) +
  geom_line() + 
  geom_ribbon(aes(ymin = lci, ymax = uci, fill = smi_status), color=NA, alpha = 0.2) +
  facet_grid(condition + sex ~ smi, scales = "free_y") + 
  theme_bw(
    base_size = 18 # default is 11
  ) +
  theme(
    strip.background = element_rect(fill = "White"),
    panel.grid = element_blank(),
    axis.text.x=element_text(angle = 90, vjust = 0.5)
  ) + 
  labs(
    x = "Year", 
    y = "Age-standardised incidence rate per 1000 person-years",
    color = "", fill = ""
  )

ggsave(
  file = file.path(fig_folder, "age_standardised_smi_sex_plot_v3.pdf"),
  width = 10, 
  height = 15
)
