#
# Forest style plot of key results
#
# K.Fleetwood
# 25 November 2024
# 

#
# 1. Set-up --------------------------------------------------------------------
#

library(tidyverse)
library(ggh4x)

incidence_folder <- 
  "U:\\Datastore\\CMVM\\smgphs\\groups\\v1cjack1-CSO-SMI-MI\\Incidence"

res_folder <- file.path(incidence_folder, "Results") 
fig_folder <- file.path(incidence_folder, "Figures")


#
# 2. Load data ----------------------------------------------------------------
#

# 2.1 Main models -------------------------------------------------------------

# MI
mi_mod_1a <- 
  read.csv(
    file.path(
      res_folder,
      "05_mi_1_mod_1a_coef_2024-10-23.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Myocardial infarction",
    pop = "Overall"
  )
  
mi_mod_1a_f <- 
  read.csv(
    file.path(
      res_folder,
      "05_mi_1_mod_1a_coef_female2024-10-23.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Myocardial infarction",
    pop = "Female"
  )

mi_mod_1a_m <- 
  read.csv(
    file.path(
      res_folder,
      "05_mi_1_mod_1a_coef_male2024-10-23.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Myocardial infarction",
    pop = "Male"
  )

# Heart failure

hf_mod_1a <- 
  read.csv(
    file.path(
      res_folder,
      "05_heart_failure_1_mod_1a_coef_2024-10-24.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Heart failure",
    pop = "Overall"
  )

hf_mod_1a_f <- 
  read.csv(
    file.path(
      res_folder,
      "05_heart_failure_1_mod_1a_coef_female2024-10-24.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Heart failure",
    pop = "Female"
  )

hf_mod_1a_m <- 
  read.csv(
    file.path(
      res_folder,
      "05_heart_failure_1_mod_1a_coef_male2024-10-24.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Heart failure",
    pop = "Male"
  )

# Stroke

strk_mod_1a <- 
  read.csv(
    file.path(
      res_folder,
      "05_stroke_1_mod_1a_coef_2024-10-25.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Stroke",
    pop = "Overall"
  )

strk_mod_1a_f <- 
  read.csv(
    file.path(
      res_folder,
      "05_stroke_1_mod_1a_coef_female2024-10-25.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Stroke",
    pop = "Female"
  )

strk_mod_1a_m <- 
  read.csv(
    file.path(
      res_folder,
      "05_stroke_1_mod_1a_coef_female2024-10-25.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Stroke",
    pop = "Male"
  )


# Bind results for mod_1a
mod_1a <- 
  bind_rows(
    mi_mod_1a,
    mi_mod_1a_f,
    mi_mod_1a_m,
    hf_mod_1a,
    hf_mod_1a_f,
    hf_mod_1a_m,
    strk_mod_1a,
    strk_mod_1a_f,
    strk_mod_1a_m
  )

smi_lvls <- c("Schizophrenia", "Bipolar disorder", "Depression")
cond_lvls <- c("Myocardial infarction", "Heart failure", "Stroke")
pop_lvls <- c("Overall", "Female", "Male")

mod_1a <- 
  mod_1a %>%
  mutate(
    smi = 
      case_when(
        X %in% "smisch" ~ "Schizophrenia",
        X %in% "smibd" ~  "Bipolar disorder",
        X %in% "smidep" ~ "Depression"
      ),
    smi = factor(smi, levels = smi_lvls),
    condition = factor(condition, levels = cond_lvls),
    pop = factor(pop, levels = pop_lvls)
  )


# 2.2 Sensitivity analysis (including ethnicity) ------------------------------

# MI
mi_mod_2a <- 
  read.csv(
    file.path(
      res_folder,
      "05_mi_1_mod_2a_coef_2024-10-23.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Myocardial infarction"
  )

# Heart failure

hf_mod_2a <- 
  read.csv(
    file.path(
      res_folder,
      "05_heart_failure_1_mod_2a_coef_2024-10-24.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Heart failure"
  )

# Stroke

strk_mod_2a <- 
  read.csv(
    file.path(
      res_folder,
      "05_stroke_1_mod_2a_coef_2024-10-25.csv"
    )
  ) %>%
  filter(
    substring(X, 1, 3) %in% "smi"
  ) %>%
  mutate(
    condition = "Stroke"
  )

mod_2a <- 
  bind_rows(
    mi_mod_2a,
    hf_mod_2a,
    strk_mod_2a
  )

mod_2a <- 
  mod_2a %>%
  mutate(
    smi = 
      case_when(
        X %in% "smisch" ~ "Schizophrenia",
        X %in% "smibd" ~  "Bipolar disorder",
        X %in% "smidep" ~ "Depression"
      ),
    smi = factor(smi, levels = smi_lvls),
    condition = factor(condition, levels = cond_lvls)
  )

#
# 3. Forest plots -------------------------------------------------------------
#

# 3.1 Main models -------------------------------------------------------------

jitter <- 0.35

mod_1a <- 
  mod_1a %>%
  mutate(
    pos = 
      case_when(
        condition %in% "Myocardial infarction" & pop %in% "Overall" ~ 4 + jitter,
        condition %in% "Myocardial infarction" & pop %in% "Female"  ~ 4,
        condition %in% "Myocardial infarction" & pop %in% "Male"    ~ 4 - jitter,
        condition %in% "Heart failure"         & pop %in% "Overall" ~ 2 + jitter,
        condition %in% "Heart failure"         & pop %in% "Female"  ~ 2,
        condition %in% "Heart failure"         & pop %in% "Male"    ~ 2 - jitter,
        condition %in% "Stroke"                & pop %in% "Overall" ~ 0 + jitter,
        condition %in% "Stroke"                & pop %in% "Female"  ~ 0,
        condition %in% "Stroke"                & pop %in% "Male"    ~ 0 - jitter
      )
    )

scales_1a <- 
  list(
    scale_x_log10(limits = c(NA, 2.7), name = "Incidence rate ratio (95% CI)"),
    scale_x_log10(limits = c(NA, 2.7), name = "Incidence rate ratio (95% CI)"),
    scale_x_log10(limits = c(NA, 1.7), name = "Incidence rate ratio (95% CI)") 
  )

fig_1a <- 
  ggplot(mod_1a, aes(irr, pos, col = pop, label = irr_fmt)) +
  geom_point(size = 0.5) +
  geom_text(vjust=-0.5, show.legend = FALSE) +
  geom_errorbarh(aes(xmin = irr_low, xmax = irr_upp, height = 0)) +
  geom_vline(xintercept = 1, col="lightgrey") +
  facet_wrap(vars(smi), ncol = 3, scales = "free_x") +
  facetted_pos_scales(x = scales_1a) + 
  scale_y_continuous(
    breaks = c(4, 2, 0),
    limits = c(NA, 4.6),
    labels = c("Myocardial infarction", "Heart failure", "Stroke")
  ) +
  theme_bw() +
  theme(
    axis.line.x.bottom = element_line(size=0.5),
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    legend.title=element_blank()
  )

fig_1a 

ggsave(
  file.path(fig_folder, "Fig_IRR_main_v2.pdf"),
  height = 5,
  width = 9, 
  units = "in"
)

# 3.2 Sensitivity analysis (including ethnicity) ------------------------------

mod_2a <- 
  mod_2a %>%
  mutate(
    pos = 
      case_when(
        condition %in% "Myocardial infarction"     ~ 4,
        condition %in% "Heart failure"  ~ 2,
        condition %in% "Stroke"        ~ 0,
      )
  )

fig_2a <- 
  ggplot(mod_2a, aes(irr, pos, label = irr_fmt)) +
  geom_point(size = 0.5) +
  geom_text(vjust=-0.5, show.legend = FALSE) +
  geom_errorbarh(aes(xmin = irr_low, xmax = irr_upp, height = 0)) +
  geom_vline(xintercept = 1, col="lightgrey") +
  facet_wrap(vars(smi), ncol = 3, scales = "free_x") +
  facetted_pos_scales(x = scales_1a) + 
  #scale_x_log10(limits = c(NA, 2.6), name = "Incidence rate ratio (95% CI)") + 
  scale_y_continuous(
    breaks = c(4, 2, 0),
    limits = c(NA, 4.6),
    labels = c("Myocardial infarction", "Heart failure", "Stroke")
  ) +
  theme_bw(
    base_size = 14
  ) +
  theme(
    axis.line.x.bottom = element_line(size=0.5),
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white")
  )

fig_2a 

ggsave(
  file.path(fig_folder, "Fig_IRR_sensitvity_v2.pdf"),
  height = 3.5,
  width = 9, 
  units = "in"
)
