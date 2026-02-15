# df-ctc-live
Public repository of all code relating to the data fabric CTC telemetry demo.

# Instructions on demo setup
## Pre-requisites
You must have a Windows rig with iRacing installed and a HPE Data Fabric cluster (latest tested version 8.0 - older versions should also work fine).

## High-level Demo Architecture
The end-state of this demo consists of 2 dashboards for displaying the ability of HPE Data Fabric to serve realtime and historical analytics from a single intelligent data platform that stores, ingests & transforms data both in realtime and batch mode. 

The iRacing rig produces racing telemetry data in realtime that is exposed via a websocket. 

A producer script located on the data fabric (or external client) subscribes to the websocket and pushes data live & in-sequence to a HPE Data Fabric Stream.

After an iRacing session is completed, all telemetry data (which includes GPS data - not exposed during live race-time) is stored into a proprietary binary file format (.ibt)

There is a script included here that runs on the iRacing host and uploads (on a periodic basis, eg. daily) all new ibt files generated to the HPE Data Fabric Object Store.

Then there are further scripts which will run on the HPE Data Fabric to ingest all IBT telemetry data into a master binary table (via HBASE REST API). Two further jobs then run on a schedule to populate a "best lap" table and "leaderboard" table. 

Finally there are two dashboard scripts (streamlit applications).

- Streamlit application 1 provides a real-time analytics dashboard. This takes data from the live HPE Data Fabric stream.

- Streamlit application 2 provides a historical analytics dashboard including telemetry from the best lap on record for the track of choice, and a top-10 leaderboard for the chosen track. This takes data from the HPE Data Fabric binary tables populated with IBT files. 

## Instructions

Please first configure the windows machine with iRacing and the scripts found in folder 00 above. Then configure the data fabric cluster with the scripts found in folder 01 above. Note that you will need to manually configure automation of the various jobs (like ibt file upload and re-population of best lap & leaderboard tables based on your preference). For immediate update of the historical dashboard, simply manually run the corresponding scripts. 



