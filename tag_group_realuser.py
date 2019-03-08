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

def get_mr_job_type(app_obj):
    if app_obj.getAppConf("pig.version"):
        return "mapreduce-pig"
    elif app_obj.getAppConf("hive.exec.plan"):
        return "mapreduce-hive"
    elif app_obj.getAppConf("mapred.map.runner.class") and app_obj.getAppConf("mapred.map.runner.class") == "org.apache.hadoop.streaming.PipeMapRunner":
        return "mapreduce-streaming"
    elif app_obj.getAppConf("distcp.job.dir"):
        return "mapreduce-distcp"
    elif app_obj.getAppConf("cascading.app.id"):
        return "mapreduce-cascading"
    else:
        return "mapreduce-other"

def get_tags(app_obj):
    if app_obj is None:
        return None

    tags = {}

    realuser = None
    job_type = None

    group = get_group_tag(app_obj.getQueue())
    app_type = app_obj.getAppType()

    if app_type == 'mr':
        realuser = app_obj.getAppConf("hive.sentry.subject.name")
        job_type = get_mr_job_type(app_obj)

    if realuser:
        tags['realuser'] = realuser
    else:
        tags['realuser'] = app_obj.getUsername()

    if job_type:
        tags['jobtype'] = job_type

    if group:
        tags['group'] = group

    return tags
