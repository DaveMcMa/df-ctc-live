# In this directory you will find all required scripts which should run on the Data Fabric cluster to ingest data and display dashboards via Streamlit

Important notes:

1 - HbaseRest.py script must be saved in the same directory as the base scripts, as this is called on by other scripts.  

2 - Most scripts require you to have a valid ticket to the data fabric, even when running within the cluster - make sure you run these scripts as user with high priveleges to all resources (usually mapr) - using command "maprlogin password" for example  

3 - Historical records are stored in a HPE Data Fabric binary table - scripts here leverage the HBASE REST API server from the HPE Data Fabric - make sure this service is installed & referenced correctly.  

4 - As we are using python libraries to interact with streams & tables, make sure you have the HPE Data Fabric python client configured correctly (https://docs.ezmeral.hpe.com/datafabric-customer-managed/80/AdvancedInstallation/InstallingStreamsPYClient.html?hl=python%2Cstreams)  


## Data Loader scripts
The purpose of these scripts is to load live data into a data fabric topic and historical data into a data fabric table

### df_load_topic
This is a producer scripts that listens to the websocket on the iRacing host for telemetry and pushes all entries in sequence into a HPE Data Fabric Stream.

### df_load_table
This is a script that searches for new IBT files uplaoded into a Data Fabric bucket and loads all new data into a master telemetry HPE Data Fabric Binary Table.

## Daily jobs
The purpose of these scripts is to run daily jobs to update dedicated binary tables for best laps and overall leaderboards based on the master telemetry table. This saves our frontend from having to perform full table scans (a very expensive operation).

### df_job_bestlap
This is a job which scans the master table and populates a bestlap table with the best lap from each discovered track on record.

### df_job_leaderboard
This is a job which scans the master table and populates a leaderboard table with the 10 best laptimes and racer names from each track on record. 

## Dashboards (can be run on external clients also)
The purpose of these scripts is to launch two frontend dashboards to be displayed on the demo floor. 

### df_frontend_live
This is a streamlit application which listens to the live HPE Data Fabric stream and displays telemetry in real time back to the driver / analyst

### df_frontend_table
This is a streamlit application which scans both the bestlap and leaderboard tables and displays telemetry from the best lap on record & the top ten lap times for the corresponding track

