# Data Pipeline and Analysis of Belgian Car Data

This project aims to analyze the interaction between various municipal-level characteristics and the
degree of car ownership in Belgium. To achieve this, we have developed a comprehensive
workflow that facilitates the extraction, transformation, and analysis of relevant data.
The workflow begins with the extraction of data from online sources. This data is subsequently
ingested into our Data Lake.

Next, we perform the transformation and loading of this data. Using batch-processing and
transformations, after which we load the data into our Data Warehouse.

Finally, we connect an interactive dashboard to the Data Warehouse. This allows us to generate
insightful reports and conduct detailed analyses of the data.
The primary research question guiding this project is: How do different municipal-level
characteristics interact with the degree of car ownership? Key elements of our analysis include
population density, province, city area size, the count of sub-municipalities, and household types.

Our data sources for this analysis include car ownership data from Statbel (statbel.fgov.be) and city demographic information from Wikipedia tables.

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



