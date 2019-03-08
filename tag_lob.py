realuser_2_lob_map = {}
filename = 'Team_Org-Table.csv'

queue_prefix = "root."
queue_prefix_length = len(queue_prefix)

def get_group_tag(queue):
    group = None
    if queue and queue.startswith(queue_prefix):
        group_end_index = queue.rfind('.')
        if group_end_index > -1:
            # '.' is before queue_prefix_length, then queue is root.FOO, else root.FOO.<user>
            if group_end_index < queue_prefix_length:
                group = queue[queue_prefix_length:]
            else:
                group = queue[queue_prefix_length:group_end_index]

    return group

def populate_realuser_2_lob_map():
    with open(filename) as fp:
        array = [line.split(',') for line in fp.readlines()]
        for people in array[1:]:
            realuser_2_lob_map[people[3].strip()] = people[0].strip()

def get_mr_job_type(app_obj):
    if app_obj.getAppConf('pig.version'):
        return 'mapreduce-pig'
    elif app_obj.getAppConf('hive.exec.plan'):
        return 'mapreduce-hive'
    elif app_obj.getAppConf('mapred.map.runner.class') and app_obj.getAppConf('mapred.map.runner.class') == 'org.apache.hadoop.streaming.PipeMapRunner':
        return 'mapreduce-streaming'
    elif app_obj.getAppConf('distcp.job.dir'):
        return 'mapreduce-distcp'
    elif app_obj.getAppConf('cascading.app.id'):
        return 'mapreduce-cascading'
    else:
        return 'mapreduce-other'

def normalize_username(user):
    index = user.find('@')
    if index != -1:
        user = user[:index]
    return user
 
def get_tags(app_obj):
    if app_obj is None:
        return None
 
    tags = {}
    realuser = None
    job_type = None
    app_type = app_obj.getAppType()
    group = get_group_tag(app_obj.getQueue())
 
    if app_type == 'mr':
        realuser = app_obj.getAppConf('hive.sentry.subject.name')
        job_type = get_mr_job_type(app_obj)

    if not realuser:
        realuser = app_obj.getUsername()

    if realuser:
        realuser = normalize_username(realuser)
        tags['realuser'] = realuser
        if realuser in realuser_2_lob_map:
            tags['lob'] = realuser_2_lob_map[realuser]
 
    if job_type:
        tags['jobtype'] = job_type
 
    if group:
        tags['group'] = group

    return tags

populate_realuser_2_lob_map()

