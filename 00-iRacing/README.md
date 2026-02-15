# This folder contains all required scripts hosted on the iRacing machine

In this folder are two scripts to be hosted on the iRacing machine:

## dataserve.py

This script opens a websocks on the iracing host, listens for an active iracing session, and once found broadcasts selected live telemetry via a websocket

## ibt_dfupload.py

This script scans the local iRacing directory for ibt files (binary format which contains all telemetry data for a given session) and uploads any new files to the HPE Data Fabric object store. The script includes logic to check the target bucket for existing files and does not re-upload any files which already exist on the bucket. 
