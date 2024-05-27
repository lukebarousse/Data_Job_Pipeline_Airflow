![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)

# 🤓 Data Job Pipeline w/ Airflow 
![Airflow DAG](/extra/airflow_graph.png)
What up, data nerds! This is a data pipeline I built that moves Google job search data from [SerpApi](https://serpapi.com/) to a BigQuery database.


## Background
I built an [app](https://jobdata.streamlit.app/) to open-source job requirements to aspiring data nerds so they can more efficiently focus on the skills they need to know for their job.  This airflow pipeline collects the data for this app.  
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://jobdata.streamlit.app/)
![dashboard](/extra/dashboard.png)

# ☝🏻 Prerequisites
- [Docker](https://docs.docker.com/get-docker/) installed on your server/machine

SERVER NOTE: I used a QNAP machine for my server, here's prereq's for that:
-  [Familiar with QNAP instructions](https://www.qnap.com/en/how-to/faq/article/how-do-i-access-my-qnap-nas-using-ssh) 
- Enable SSH via Control Panel
- Setup *Container/* folder via Container Station 

# 📲 Install
## Docker-Compose install via SSH 


1. Access server via SSH 
```
ssh admin@192.168.1.131
```
2. Change directory to main directory to add cloned directory
```
cd ..
cd share/Container
```
3. Add this repository to that directory
```
git clone https://github.com/lukebarousse/Data_Job_Pipeline_Airflow.git
```
3. Change directory into root and add environment variable for start-up
```
cd Data_Job_Pipeline_Airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

# ㊙️ Secret Keys
## Prereq
1. Access server via SSH 
```
ssh admin@192.168.1.131
```
2. Change directory to *config/* directory
```
cd ..
cd dags/config
```
## SerpApi key
Prerequisite: SerpAPI account created with enough credits
1. Get your private API key from from [SerpApi Dashboard](https://serpapi.com/dashboard)
2. Create python file for SerpApi key
```
echo -e "serpapi_key = '{Insert your key here}'" > config.py # or /dag/config/config.py if in root
```
## BigQuery Access
Prerequisite: Empty BigQuery database created with [this schema](/extra/bigquery_schema.json)
1. Follow [Google Cloud detailed documentation](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries) to:
- Enable BigQuery API
- Create a service account
- Create & get service account key (JSON file)
2. Place JSON file in [/dags/config](/dags/config/) directory
3. Add location of JSON file in [docker-compose.yaml](docker-compose.yaml)
```
environment:
    GOOGLE_APPLICATION_CREDENTIALS: './dags/config/{Insert name of JSON file}.json'
```
4. Add name of json table id to config file
```
echo -e "table_id_json = '{PROJECT_ID}.{DATASET}.{TABLE}'" >> config.py
```
I also have this as backup:
```
echo -e "table_id = '{PROJECT_ID}.{DATASET}.{TABLE}'" >> config.py
```

# 🐳 Start & Stop
Reference: [Airflow with Docker-Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
## Start-up of Docker-Compose
NOTE: If you don't want to use credits on first run change 'serpapi_biquery.py' to testing
1. If necessary, SSH and cd into root directory
```
cd ..
cd share/Container/Data_Job_Pipeline_Airflow
```
2. If first time, initialize database for airflow
```
docker-compose up airflow-init
```
3. Start aiflow
```
docker-compose up
```

## Shutdown & removal of Docker-Compose
1. If necessary, SSH and cd into root directory
```
cd ..
cd share/Container/Data_Job_Pipeline_Airflow
```
2. Stop and delete containers, delete volumes with database data and download images
```
docker-compose down --volumes --rmi all
```

# 🫁 Appendix
### Want to contribute?  
- **Data Analysis:** Share any interesting insights you find from the [dataset](https://www.kaggle.com/datasets/lukebarousse/data-analyst-job-postings-google-search) to this [subreddit](https://www.reddit.com/r/DataNerd/) and/or [Kaggle](https://www.kaggle.com/code/lukebarousse/eda-of-job-posting-data).  
- **Dashboard Build:** Contribute changes to the dashboard by using [that repo to fork and open a pull request](https://github.com/lukebarousse/Data_Analyst_Streamlit_App_V1).
- **Data Pipeline Build:** Contribute changes to this pipeline by using [this repo to fork and open a pull request](https://github.com/lukebarousse/Data_Job_Pipeline_Airflow)
---
### About the project
- Background on app 📺 [YouTube](https://www.youtube.com/lukebarousse)
- Data provided via 🤖 [SerpApi](https://serpapi.com/)
