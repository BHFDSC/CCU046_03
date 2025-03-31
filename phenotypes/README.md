# ICD-10 codelists

We used existing ICD-10 codelists. 

For **schizophrenia**, **bipolar disorder**, **depression** and **stroke** we used codelists previously published by the authors in Jackson CA, Kerssens J, Fleetwood K, Smith DJ, Mercer SW, Wild SH. Incidence of ischaemic heart disease and stroke among people with psychiatric disorders: retrospective cohort study. The British Journal of Psychiatry. 2020;217(2):442-449. doi:10.1192/bjp.2019.250. 

For **myocardial infarction** we used a codelist previously published by the authors in Fleetwood, K., Wild, S.H., Smith, D.J. et al. Severe mental illness and mortality and coronary revascularisation following a myocardial infarction: a retrospective cohort study. BMC Med 19, 67 (2021). https://doi.org/10.1186/s12916-021-01937-2. 

For **heart failure** we used a codelist available from the HDR UK Phenotype Library: Rochelle Knight, Venexia Walker, Samantha Ip, Jennifer A Cooper, Thomas Bolton, Spencer Keene, Rachel Denholm, Ashley Akbari, Hoda Abbasizanjani, Fatemeh Torabi, Efosa Omigie, Sam Hollings, Teri-Louise North, Renin Toms, Emanuele Di Angelantonio, Spiros Denaxas, Johan H Thygesen, Christopher Tomlinson, Ben Bray, Craig J Smith, Mark Barber, George Davey Smith, Nishi Chaturvedi, Cathie Sudlow, William N Whiteley, Angela Wood, Jonathan A C Sterne. PH968 / 2146 - CCU002_01 Heart failure. Phenotype Library [Online]. 17 May 2022. Available from: http://phenotypes.healthdatagateway.org/phenotypes/PH968/version/2146/detail/. 

# SNOMED codelists

The General Practice Extraction Service Data for Pandemic Planning and Research (GDPPR) dataset includes a subset of all SNOMED codes used in the United Kingdom. The subset of codes included is defined by the primary care domain (PCD) reference set [1]. The PCD reference set is also used by the Quality and Outcomes Framework (QOF) and current and historical versions of the reference set are available from NHS England [2]. The available SNOMED codes are grouped into code clusters [1].

## SNOMED codelists for mental illness
To develop our SNOMED codelists for mental illness we extracted codes from all of the clusters related to relevant mental illness diagnoses: DEPR_COD (depression diagnosis codes), DEPRES_COD (depression resolved codes) and MH_COD (psychosis and schizophrenia and bipolar affective disease codes) from version 47.1 of the QOF cluster list (which was the most recent version at the time of developing the SMI codelists) [3]. Additionally, we mapped existing Read v2 and Clinical Terms Version 3 codelists [4] for schizophrenia, bipolar disorder and depression to SNOMED, and identified codes available within the GDPPR dataset. We collated all SNOMED codes identified from the clusters, and from the mapping process and then identified codes for each of schizophrenia, bipolar disorder or depression. 

## SNOMED codelists for cardiovascular disease
We identified existing SNOMED codelists from the HDR UK phenotype library for each of our cardiovascular disease (CVD) outcomes: myocardial infarction [5], heart failure [6] and stroke [7]. Each of these existing codelists was developed for use with the CVD-COVID-UK resource, however they were based on earlier versions of the PCD reference set. We adapted these codelists for use in our study by reviewing the codes, excluding codes that were not relevant to our study (for example, we wanted to include only current CVD events, so we excluded “history of codes” such as “H/O: Myocardial infarction in last year”), and adding additional relevant codes from the CHD_COD (coronary heart disease codes), HF_COD (heart failure codes), STRK_COD (stroke diagnosis codes), HSTRK_COD (haemorrhagic stroke codes) and OSTR_COD (non-haemorrhagic stroke codes) clusters from version 49.1 of the QOF cluster list (which was the most recent version at the time of developing the CVD codelists) [8].

# HDR UK Phenotype Library

All of our codelists are also available via the HDR UK Phenotype Library:

* Schizophrenia: https://phenotypes.healthdatagateway.org/phenotypes/PH1718
* Bipolar disorder: https://phenotypes.healthdatagateway.org/phenotypes/PH1719
* Depression: https://phenotypes.healthdatagateway.org/phenotypes/PH1720
* Myocardial infarction: https://phenotypes.healthdatagateway.org/phenotypes/PH1722
* Heart failure: https://phenotypes.healthdatagateway.org/phenotypes/PH1721
* Stroke: https://phenotypes.healthdatagateway.org/phenotypes/PH1723

# References

1.	NHS Digital. General Practice Extraction Service (GPES) Data for Pandemic Planning and Research (GDPPR): a guide for analysts and users of the data. 2024. Available: https://digital.nhs.uk/coronavirus/gpes-data-for-pandemic-planning-and-research/guide-for-analysts-and-users-of-the-data (accessed 13 Jan 2025).
2.	NHS Digital. Quality and Outcomes Framework (QOF) business rules. 2024. Available: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/business-rules (accessed 13 Jan 2025)
3.	NHS Digital. Quality and Outcomes Framework (QOF) business rules v47.0 2022-2023. 2023. Available: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/quality-and-outcome-framework-qof-business-rules/quality-and-outcomes-framework-qof-business-rules-v47.0-2022-2023 (accessed 13 Jan 2025).
4.	Prigge R, Fleetwood KJ, Jackson CA, et al. Robustly Measuring Multiple Long-Term Health Conditions Using Disparate Linked Datasets in UK Biobank. Available at SSRN: https://ssrn.com/abstract=4863974.
5.	Wood A, Denholm R, Hollings S, et al. PH942 / 2120 - CCU000 Acute Myocardial Infarction (AMI). Phenotype Library. 2022. Available: http://phenotypes.healthdatagateway.org/phenotypes/PH942/version/2120/detail/. 
6.	Knight R, Walker V, Ip S, et al. PH968 / 2146 - CCU002_01 Heart failure. Phenotype Library. 2022. Available: http://phenotypes.healthdatagateway.org/phenotypes/PH968/version/2146/detail/.
7.	Wood A, Denholm R, Hollings S, et al. PH948 / 2126 - CCU000 Stroke. Phenotype Library. 2022. Available: http://phenotypes.healthdatagateway.org/phenotypes/PH948/version/2126/detail/.
8.	NHS Digital. Quality and Outcomes Framework (QOF) business rules v49 2024-2025. 2024. Available: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/business-rules/quality-and-outcomes-framework-qof-business-rules-v49-2024-25 (accessed 13 Jan 2025).


