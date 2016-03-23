'''
LambdaAutomatedBackup.py
Created on 22 Mar 2016

@author: Nik Shaw
'''
import boto3
import logging
import datetime
import time


#setup simple logging for INFO
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

#define the connection - we are using both a client and a resource connection
ec2 = boto3.client('ec2', region_name="eu-west-1")
snapcon = boto3.resource('ec2', region_name="eu-west-1")

#set the snapshot removal offset
cleanDate = datetime.datetime.now()-datetime.timedelta(days=5)

def lambda_handler(event, context):
# Create a list of all the volumes to be backed up
# These are determined by the value of the tag 'Backup' being set to Yes or True
    backupVolumes = ec2.describe_volumes(
        Filters=[
            {'Name': 'tag-key', 'Values': ['Backup']},
            {'Name': 'tag-value', 'Values': ['Yes','yes','True','true']}
        ]
        )['Volumes']


# loop for each volume in the backup list    
    for vol in backupVolumes:
        tempTags=[]
        vol_id = vol['VolumeId']

        for t in vol['Tags']:                
#           pull the name tag
                if t['Key'] == 'Name':
                    instanceName =  t['Value']
                    tempTags.append(t)
                else:
                    tempTags.append(t)
                        
# Set the Snapshot description    
        description = str(datetime.datetime.now()) + "-" + instanceName + "-" + vol_id + "-automated"
        snapshot = snapcon.create_snapshot(VolumeId=vol_id, Description=description)
        tags = snapshot.create_tags( 
                Tags = tempTags 
            )    
        print("[LOG] snapshot created " + str(snapshot))

#clean up old snapshots
    print("[LOG] Cleaning out old entries starting on " + str(cleanDate))
    for snap in snapcon.snapshots.all():
#verify results have a value
        if snap.description.endswith("-automated"): 
            
#Pull the snapshot date
            snapDate = snap.start_time.replace(tzinfo=None)
            print("[DEBUG] Snap date= " + str(snapDate))
            print("[DEBUG] Clean-up Date =" + str(cleanDate))

            
#Compare the clean dates
            if cleanDate > snapDate:
                print("[INFO] Deleting: " + snap.id + " - From: " + str(snapDate))
                try:
                    snapshot = snap.delete()
                except:
#if we timeout because of a rate limit being exceeded, give it a rest of a few seconds
                    print("[INFO]: Waiting 5 Seconds for the API to cooldown")
                    time.sleep(5)
                    snapshot = snap.delete()
                print("[INFO] " + str(snapshot))


