import random

dept = ["Sales", "Engineering", "Data Scientist", "AI", "Data Analyist", "Operations"]
project = ["Operations","Product Management","Marketing","Product Development","Sales","Data Analyist","Finance","Data Ops","ML","AI"]
team = ["Data Ops", "Engineering", "Data Analyist", "Data Scientist", "ML Ops", "AI Ops"]

realuser_2_group_map = {}
#filename = '/usr/local/unravel/etc/Team_Org-Table.csv'
filename = '/tmp/Team_Org-Table.csv'

queue_prefix_1 = "root."
queue_prefix_1_length = len(queue_prefix_1)


def get_queue_tag(queue):
    queue_tag = None
    if queue:
        if queue.startswith(queue_prefix_1):
            queue_end_index = queue.rfind('.')
            if queue_end_index > -1:
                # '.' is before queue_prefix_length, then queue is root.FOO, else root.FOO.<user>
                if queue_end_index < queue_prefix_1_length:
                    queue_tag = queue[queue_prefix_1_length:]
                else:
                    queue_tag = queue[queue_prefix_1_length:queue_end_index]
        elif '-' in queue:
            queue_tag = queue.split('-')[1]
        else:
            queue_tag = queue
    return queue_tag


def populate_realuser_2_group_map():
    with open(filename) as fp:
        array = [line.split(',') for line in fp.readlines()]
        for people in array[1:]:
            realuser_2_group_map[people[3].strip()] = people[0].strip()
            

def get_mr_job_type(app_obj):
    if app_obj.getAppConf('pig.version'):
        return 'mapreduce-pig'
    elif app_obj.getAppConf('hive.exec.plan'):
        return 'mapreduce-hive'
    elif app_obj.getAppConf('mapred.map.runner.class') and \
            app_obj.getAppConf('mapred.map.runner.class') == 'org.apache.hadoop.streaming.PipeMapRunner':
        return 'mapreduce-streaming'
    elif app_obj.getAppConf('distcp.job.dir'):
        return 'mapreduce-distcp'
    elif app_obj.getAppConf('cascading.app.id'):
        return 'mapreduce-cascading'
    else:
        return 'mapreduce-other'


def get_spark_job_type(app_obj):
    job_type = app_obj.getAppConf("spark.unravel.appType")
    if job_type and "Hive on Spark" not in app_obj.getAppConf('spark.app.name'):
        return job_type
    elif job_type and "Hive on Spark" in app_obj.getAppConf('spark.app.name'):
        return "hiveOnSpark"
    else:
        return "spark"


def get_tez_job_type(app_obj):
    if get_queue_tag(app_obj.getQueue()) == "LLAP":
        return "tez-LLAP"
    else:
        return "tez-default"


def normalize_username(user):
    index = user.find('@')
    if index != -1:
        user = user[:index]
    return user
  

def get_db(app_obj):
    db = None
    table = None

    it = app_obj.getInputTables()
    if it and len(it) > 0:
        table = it[0]

    if not table:
        ot = app_obj.getOutputTables()
        if ot and len(ot) > 0:
            table = ot[0]

    if table:
        dot_index = table.find('.')
        db = table if dot_index == -1 else table[:dot_index]
    return db


def get_tags(app_obj):
    if app_obj is None:
        return None

    tags = {}
    job_type = None
    app_type = app_obj.getAppType()
    queue = get_queue_tag(app_obj.getQueue())
    real_user = app_obj.getRealUser()

    tags['dept'] = dept[random.randint(0, 5)]
    tags['project'] = project[random.randint(0, 9)]
    tags['team'] = team[random.randint(0, 5)]

    if app_type == 'mr':
        job_type = get_mr_job_type(app_obj)
    elif app_type == 'spark':
        job_type = get_spark_job_type(app_obj)
    elif app_type == 'tez':
        job_type = get_tez_job_type(app_obj)

    if not real_user:
        real_user = app_obj.getUsername()
  
    if real_user:
        real_user = normalize_username(real_user)
        tags['RealUser'] = real_user
        if real_user in realuser_2_group_map:
            tags['Group'] = realuser_2_group_map[real_user]

    if job_type:
        tags['JobType'] = job_type

    if queue:
        tags['Queue'] = queue

    db = get_db(app_obj)
    if db:
        tags['DB'] = db

    return tags


populate_realuser_2_group_map()
