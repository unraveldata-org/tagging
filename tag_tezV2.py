import json
import subprocess

# ATS Server Information
ATS_URL = "https://(hostname):8190"
ATS_TEZ_URL = ATS_URL + "/ws/v1/timeline/TEZ_APPLICATION/tez_"
ATS_HIVE_URL = ATS_URL + "/ws/v1/timeline/HIVE_QUERY_ID/"

# Retrieve configured Tez tagging data from ATS
def get_json(url):
        cmd = "curl --negotiate -u a:b " + url
        args = cmd.split()
        process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return json.loads(stdout)

# Function to retrieve and process tagging data. Default value is "Untagged" for hivelob tag. Catch connection error exception
# and return as API.connection.error
def get_tags(app):
        tags = {}
        try:
                hivelob = None
                print ("app id " + app.getAppId() + " type " + app.getAppType())

                try:
                        if app.getAppType() == 'tez':
                                appid = app.getAppId()
                                hivelob = get_json(ATS_TEZ_URL + appid)['otherinfo']['config']['edl.lob']
                        if app.getAppType() == 'mr':
                                hivelob = app.getAppConf("edl.lob")
                        if hivelob = None
                                hivelob = 'Untagged'

                except Exception as (ex):
                       print type(ex), ex
                       hivelob = 'API.connection.error'


                tags['edl.lob'] = hivelob
                print("Tag for app " + appid + " is " +str(tags))

        except Exception as (ex):
                print type(ex), ex

        return tags
