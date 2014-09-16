__author__ = 'mcowger'

import logging
import time

from hammock import Hammock as rp4vm_hammock
from hammock import Hammock as vcops_hammock
from pprint import pprint,pformat
from collections import namedtuple

class Default(object):
    @classmethod
    def get_class_name(cls):
        return cls.__name__

    def __str__(self):
        # to show include all variables in sorted order
        showList = sorted(set(self.__dict__))

        return "<{}>@0x{}:\n".format(self.get_class_name(),id(self)) + "\n".join(["  %s: %s" % (key.rjust(16), self.__dict__[key]) for key in showList])
    def __repr__(self):
        return self.__str__()

def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = {}
    if type(structure) not in(dict, list):
        flattened[((path + "_") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, path + "|" + key, flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, path + "|" + key, flattened)
    for keyname in flattened.keys():
        new_key_name = str(keyname).replace('||',"")
        flattened[new_key_name] = flattened.pop(keyname)
    return flattened


class Vcops_Connection(Default):
    def __init__(self,options):
        self.options = options
        try:
            self.vcops = vcops_hammock(self.options['--protocol']+ "://" + self.options['VCOPS_IP'] + '/HttpPostAdapter/OpenAPIServlet',verify=False,auth=(self.options['--vcops_user'],self.options['--vcops_pass']))
        except Exception as exp:
            raise
        logging.info("Logging into vcops @ %s as %s" % (self.options['VCOPS_IP'],options['--vcops_user']))

    def submit_set(self,first_line,metric_lines,debug=False):
        logging.debug("Submitting metrics as user: %s:\n %s" % (self.options['--vcops_user'],first_line+'\n'+metric_lines[0:500]))
        if not debug:
            response = self.vcops.POST(data=first_line+'\n'+metric_lines)
            return response
        else:
            return None

class Vcops_Record_Keeper(Default):
    def __init__(self,resourceName,resourceKindKey="",identifiers="",resourceDescription=""):

        self.resource_name = resourceName
        self.resource_kind_key = resourceKindKey
        self.identifiers = identifiers
        self.resource_description = resourceDescription
        self.metrics = []

    @property
    def metric_lines(self):
        return '\n'.join(self.metrics)

    @property
    def first_line(self):
        first_line = ",".join( ( self.resource_name,'HTTP Post',self.resource_kind_key,str(self.identifiers),self.resource_description,'','')  )
        logging.debug("Returning first line: %s" % first_line)
        return first_line

    def add_metric_observation(self,entity_name,metric_name,alarm_level=0,alarm_message="",value=''):
        logging.debug("Received Parameters: {}".format(locals()))
        metric_string = ','.join(
            (metric_name,
             '',
             '',
             str(self.current_time_millis),
             str(value),
             str(alarm_level)
            )
        )
        self.metrics.append(metric_string)
        logging.debug("Added metric: %s" % metric_string)


    @property
    def current_time_millis(self):
        import time
        return int(round(time.time() * 1000))

class VirtualMachine(Default):
    def __init__(self,uuid,vcenter_uuid,cgroup_id,cluster_id,copy_id):
        self.uuid = uuid
        self.vcenter_uuid = vcenter_uuid
        self.cgroup_id = cgroup_id
        self.cluster_id = cluster_id
        self.copy_id = copy_id

class RP4VM_Connections(Default):
    def __init__(self, options):
        try:
            self.rp4vm = rp4vm_hammock(options['--protocol']+ "://" + options['RPA_IP'] + "/fapi/rest/4_1",verify=False, auth=(options['RPA_USER'], options['RPA_PASS']))
        except Exception as exp:
            raise
        self.options = options

    def get_clusters(self):
        try:
            clusters = self.rp4vm.clusters.GET().json()['clustersInformation']
            clusters = [cluster['clusterUID']['id'] for cluster in clusters]
            logging.debug("Got Cluster List: {}".format(clusters))
            return clusters
        except Exception as exp:
            raise

    def get_cluster_stats(self,cluster_id):
        try:
            cluster = self.rp4vm.clusters(cluster_id).statistics.GET().json()['traffic']
            logging.debug("Cluster Stats for ClusterID {}: {}".format(cluster_id,cluster))
            return flatten(cluster)
        except Exception as exp:
            raise

    def get_cgroup_name(self,cgroup_id):
        try:
            name = self.rp4vm.groups(cgroup_id).name.GET().json()['string']
            return name
        except Exception as exp:
            raise

    def get_cgroups(self):
        try:
            groups = self.rp4vm.groups.GET().json()['innerSet']
            group_list = [group['id'] for group in groups]
            logging.debug("Got Group List: {}".format(group_list))
            return group_list
        except Exception as exp:
            raise

    def get_cgroup_stats(self,cgroup_id):
        try:
            group_stats_raw = self.rp4vm.groups(cgroup_id).statistics.GET().json()
            return flatten(group_stats_raw)
        except Exception as exp:
            raise

    def get_cluster_name(self,cluster_id):
        try:
            cluster_name = self.rp4vm.clusters(cluster_id).settings.GET().json()['clusterName']
            return cluster_name
        except Exception as exp:
            raise

    def get_replicated_vms_by_cgroup(self,cgroup_id):
        all_vms = []
        try:
            replication_sets = self.rp4vm.groups(cgroup_id).settings.GET().json()['vmReplicationSetsSettings']
            for replication_set in replication_sets:
                for vm in replication_set['replicatedVMs']:
                    thisvm = VirtualMachine(
                        uuid = vm['vmUID']['uuid'],
                        vcenter_uuid = vm['vmUID']['virtualCenterUID']['uuid'],
                        cgroup_id = vm['groupCopyUID']['groupUID']['id'],
                        cluster_id = vm['groupCopyUID']['globalCopyUID']['clusterUID']['id'],
                        copy_id=vm['groupCopyUID']['globalCopyUID']['copyUID']
                    )
                    all_vms.append(thisvm)
            return all_vms
        except Exception as exp:
            raise

    def collect_and_submit_clusters(self,debug=False):
        vcops_connection = Vcops_Connection(options=self.options)
        for cluster_id in self.get_clusters():
            cluster_stats = self.get_cluster_stats(cluster_id)
            cluster_name = self.get_cluster_name(cluster_id)

            cluster_entry = Vcops_Record_Keeper(
                    resourceName=cluster_name,
                    resourceKindKey="RP-Cluster",
                    identifiers=cluster_id,
                    resourceDescription="RecoverPoint Cluster"
            )

            for stat in cluster_stats:
                cluster_entry.add_metric_observation(
                    entity_name=cluster_name,
                    metric_name=stat,
                    alarm_level=0,
                    alarm_message="",
                    value=cluster_stats[stat]
                )
            vcops_connection.submit_set(
                cluster_entry.first_line,
                cluster_entry.metric_lines,
                debug=debug
            )
    def collect_and_submit_cgroups(self,debug=False):
        vcops_connection = Vcops_Connection(options=self.options)
        for group_id in self.get_cgroups():
            cgroup_stats = self.get_cgroup_stats(group_id)
            cgroup_name = self.get_cgroup_name(group_id)
            cgroup_entry = Vcops_Record_Keeper(
                    resourceName=cgroup_name,
                    resourceKindKey="RP-ConsistencyGroup",
                    identifiers=group_id,
                    resourceDescription="RecoverPoint Consistency Group"
            )
            for stat in cgroup_stats:
                cgroup_entry.add_metric_observation(
                    entity_name=cgroup_name,
                    metric_name=stat,
                    alarm_level=0,
                    alarm_message="",
                    value=cgroup_stats[stat]
                )
            vcops_connection.submit_set(
                cgroup_entry.first_line,
                cgroup_entry.metric_lines,
                debug=debug
            )


