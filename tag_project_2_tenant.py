import json

tenant_queue_prefixs = ["root.UAT-","root.PROD-"]

env_conf_file = "project_2_tenant_map.json"

def load_config(config_path):
    with open(config_path, 'r') as config:
        conf = json.load(config)
        config.close()
        return conf

def get_project_tenant_tags(queue):
    tags = None
    project_2_tenant_map = load_config(env_conf_file)
    for tenant_queue_prefix in tenant_queue_prefixs:
      tenant_queue_prefix_length = len(tenant_queue_prefix)
      if queue and queue.startswith(tenant_queue_prefix):
        project_end_index = queue.rfind('.')
        if project_end_index > -1:
            # '.' is before tenant_queue_prefix_length, i.e. queue is root.FOO, not root.FOO.<user>
            if project_end_index < tenant_queue_prefix_length:
                project = queue[tenant_queue_prefix_length:]
            else:
                project = queue[tenant_queue_prefix_length:project_end_index]
            tags = {}
            tags["project"] = project
            if project in project_2_tenant_map:
                tags["tenant"] = project_2_tenant_map[project]
    if queue and queue.startswith("root.") and not tags:
        tags_list = queue.split('.')
        if len(tags_list) == 3:
           tags = {}
           tags["tenant"] = tags_list[1]
           tags["project"] = tags_list[2]
        elif queue == "root.default":
           tags = {}
           tags["tenant"] = "OPS"
           tags["project"] = "PO_SL2"
    if tags:
        if tags["project"] in project_2_tenant_map:
           tags["tenant"] = project_2_tenant_map[tags["project"]]
    return tags

def get_tags(app):
    if app:
        tags = get_project_tenant_tags(app.getQueue())
        if tags:
            print ("DEBUG: Application id " + app.getAppId() + " tags are " + str(tags))
        return tags

    return None
