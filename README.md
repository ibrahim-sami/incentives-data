# Incentives Data Pipeline
For more infomation see documentation:
https://bodhi.samasource.org/display/BA/Incentives+Data+Pipeline

Pull incentives data for 2022 under the incentives folder.

Runs **every Tuesday at 0530 AM EAT**

The data is then pushed to bigquery at:

|Datasource|Project|Dataset|Table|
|----------|----------|----------|----------|
|Incentives folder|hub-data-295911|incentives_data|(sanitized_filename)|

The job is logged and an alert in configured for any non INFO level alerts that are routed to the **_gcp-monitoring_** slack channel and relevant emails.