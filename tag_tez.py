import json
import subprocess

ATS_URL = "https://(hostname):8190"
ATS_TEZ_URL = ATS_URL + "/ws/v1/timeline/TEZ_APPLICATION/tez_"
ATS_HIVE_URL = ATS_URL + "/ws/v1/timeline/HIVE_QUERY_ID/"

def get_json(url):
	cmd = "curl --negotiate -u a:b " + url
	args = cmd.split()
	process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout, stderr = process.communicate()
	return json.loads(stdout)

def get_tags(app):
	tags = None	
	try:
		hivelob = None
		print ("app id " + app.getAppId() + " type " + app.getAppType())
		if app.getAppType() == 'tez':
			appid = app.getAppId()
			hivelob = get_json(ATS_TEZ_URL + appid)['otherinfo']['config']['edl.lob']
			tags = {}
			if hivelob != None:
				tags['lob'] = hivelob
				print ("Tag for app " + appid + " is " +str(tags))
			#else:
			#	tags['lob'] = "no_lob"
		if app.getAppType() == 'mr':
			tags = {}
			hivelob = app.getAppConf("edl.lob")
			if hivelob != None:
				tags['lob'] = hivelob
				print ("Tag for app " + appid + " is " +str(tags))
			#else:
			#	tags['lob'] = "no_lob"
	except Exception, err:
		print Exception, err
		pass
	
	return tags
	
print ("Installed Python plugin")
