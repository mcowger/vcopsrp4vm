__author__ = 'mcowger'

import logging
import time

from hammock import Hammock as rp4vm_hammock
from hammock import Hammock as vcops_hammock
from pprint import pprint,pformat



class Vcops_Connection(object):
    def __init__(self,options):
        self.options = options
        try:
            self.vcops = vcops_hammock(self.options['--protocol']+ "://" + self.options['VCOPS_IP'] + '/HttpPostAdapter/OpenAPIServlet',verify=False,auth=(self.options['--vcops_user'],self.options['--vcops_pass']))
        except Exception as exp:
            raise
        logging.info("Logging into vcops @ %s as %s" % (self.options['VCOPS_IP'],options['--vcops_user']))

    def submit_set(self,first_line,metric_lines):
        logging.debug("Submitting metrics as user: %s:\n %s" % (self.options['--vcops_user'],first_line+'\n'+metric_lines[0:500]))
        response = self.vcops.POST(data=first_line+'\n'+metric_lines)
        return response

class Vcops_Record_Keeper(object):
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
        first_line = ",".join( ( self.resource_name,'HTTP Post',self.resource_kind_key,self.identifiers,self.resource_description,'','')  )
        logging.debug("Returning first line: %s" % first_line)
        return first_line

    def add_metric_observation(self,entity_name,metric_name,alarm_level=0,alarm_message="",value=''):
        logging.debug("Received Paramters: {}".format(locals()))
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

class RP4VM_Connections(object):
    def __init__(self, options):
        try:
            self.rp4vm = rp4vm_hammock(options['--protocol']+ "://" + options['RPA_IP'] + "/fapi/rest/4_1",verify=False, auth=(options['RPA_USER'], options['RPA_PASS']))
        except Exception as exp:
            raise
        self.options = options


    def get_clusters(self):
        try:
            clusters = self.rp4vm.clusters.GET().json()['clustersInformation']
            logging.debug("Got Cluster List: {}".format(clusters))
            return clusters
        except Exception as exp:
            raise

    def get_cluster_stats(self,cluster_id):
        try:
            cluster = self.rp4vm.clusters(cluster_id).statistics.GET().json()['traffic']
            logging.debug("Cluster Stats for ClusterID {}: {}".format(cluster_id,cluster))
            return cluster
        except Exception as exp:
            raise


    def collect_and_submit(self):
        vcops_connection = Vcops_Connection(options=self.options)
        for cluster in self.get_clusters():
            cluster_id = cluster['clusterUID']['id']
            cluster_stats = self.get_cluster_stats(cluster_id)
            writes_per_second = cluster_stats['applicationIncomingWrites']
            compression_ratio = cluster_stats['connectionsCompressionRatio']
            incoming_throughput = cluster_stats['applicationThroughputStatistics']['inThroughput']
            cluster_name = cluster['clusterName']
            vcops_info = Vcops_Record_Keeper(resourceName="RP-Cluster-{}".format(cluster_name),resourceKindKey="RP-Cluster",resourceDescription="RecoverPoint Cluster")
            vcops_info.add_metric_observation(cluster_name,"writes_per_second",value=writes_per_second)
            vcops_info.add_metric_observation(cluster_name,"compression_ratio",value=compression_ratio)
            vcops_info.add_metric_observation(cluster_name,"incoming_throughput",value=incoming_throughput)
            response = vcops_connection.submit_set(vcops_info.first_line,vcops_info.metric_lines)
