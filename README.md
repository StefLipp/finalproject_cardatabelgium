# Data Pipeline and Analysis of Belgian Car Data

This project analyzes the interaction between municipal characteristics and car ownership in Belgium. We developed a workflow for extracting, transforming, and analyzing data, starting with online data extraction into a Data Lake. The data is then processed and loaded into a Data Warehouse. 

An interactive dashboard connected to the warehouse generates reports and supports detailed analysis. The main research question focuses on how municipal characteristics like population density, province, city size, sub-municipality count, and household types affect car ownership. 

Our data sources for this analysis include car ownership data from Statbel (statbel.fgov.be) and city demographic information from Wikipedia tables.

Please consult [Belgian_Car_Data_Pipeline_Documentation.pdf](https://github.com/StefLipp/finalproject_cardatabelgium/blob/main/documentation_and_design/Belgian_Car_Data_Pipeline_Documentation.pdf) for a comprehensive documentation on the Data Pipeline project, covering everything from A to Z.

An interactive online dashboard is available at https://cardatabelgium-web-dashboard.streamlit.app/ for convenient data analysis.

## Tools Used

**Languages:**         Python, SQL

**IDE:**               Jupyter Notebook, Visual Studio Code

**Workflow:**          Mage-ai

**Transformations:**   PySpark, dbt Cloud

**Infrastructure:**    Docker, Terraform

**Storage:**           Google Cloud Storage (Data Lake), Google BigQuery (Data Warehouse)

**Cloud environment:** Google Cloud

**Other Tools:**       draw.io, Bash, MS Excel, MS PowerBi


## Workflow Design

![workflow_design](https://github.com/user-attachments/assets/3c40d698-a2ce-4626-a40e-813736030782)


## Data Warehouse Design

![Screenshot 2024-09-24 at 16-03-07 ](https://github.com/user-attachments/assets/6048862e-616c-4b80-9d34-004da683661b)


## Directories
**mage_load_and_spark:** Code responsible for orchestration, data extraction, batch transformations and the loading of data.

**terraform_deploy:** Code responsible for cloud deployment.

**dbt:** Code responsible for additional data modelling.

**dashboard:** Files containing MS PowerBi dashboards.

**documentation_and_design:** Lenghty documentation and design of the pipeline.

**backup:** Zip files containing a backup of both source and processed data.

