#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import sys
import os
import time
import socket

# keys for health page
traps1 = {
  "status",
  "active_primary_shards",
  "active_shards",
  "initializing_shards",
  "relocating_shards",
  "unassigned_shards"
}

# keys for cluster stats page
traps2 = {
  "indices.docs.count",
  "indices.docs.deleted",
  "indices.flush.total",
  "indices.flush.total_time_in_millis",
  "indices.get.exists_time_in_millis",
  "indices.get.exists_total",
  "indices.get.missing_time_in_millis",
  "indices.get.missing_total",
  "indices.indexing.delete_time_in_millis",
  "indices.indexing.delete_total",
  "indices.indexing.index_time_in_millis",
  "indices.indexing.index_total",
  "indices.merges.total",
  "indices.merges.total_time_in_millis",
  "indices.refresh.total",
  "indices.refresh.total_time_in_millis",
  "indices.search.fetch_time_in_millis",
  "indices.search.fetch_total",
  "indices.search.query_time_in_millis",
  "indices.search.query_total",
  "indices.store.throttle_time_in_millis",
  "indices.warmer.total",
  "indices.warmer.total_time_in_millis",
  "jvm.mem.heap_committed_in_bytes",
  "jvm.mem.heap_used_in_bytes",
  "os.mem.actual_free_in_bytes",
  "os.mem.actual_used_in_bytes"
}


# read specified keys from json data
def getKeys(stats,traps,instance_port):
  out=''
  for t in traps:
    c=t.split('.')
    s=stats
    while len(c): s=s.get(c.pop(0),{})
    if s=={}: continue
    out += "- es.{0}.{1} {2}\n".format(instance_port,t,s)
  return out

def main(cluster='',domain='',zbx_server=''):
  # load json data
  sender = '/usr/bin/zabbix_sender'      # path zabbix_sender
  tmp = '/tmp/es_stats.tmp'              # temp file to use
  host_name=''
  node_name=''
  try:
    f = requests.get("%s/_cluster/health" % cluster, timeout=5)
    health = f.json()
    f = requests.get("%s/_nodes/_local/stats" % cluster, timeout=5)
    all = f.json()

    # only for current node
    for node_id in all['nodes']:
      host_name = all['nodes'][node_id]['host']
      node_name = all['nodes'][node_id]['name']
      node = all['nodes'][node_id]
  except:
    print "Unable to load JSON data!"
    sys.exit(1)

  port = cluster.split(':')[2]

  out = getKeys(health,traps1,port)  #getting health values
  out += getKeys(node,traps2,port)   #getting stats  values

  #debug
  print out

  tmp = "%s.%s.%s" % (tmp,node_name,port)

  try:
    with open(tmp,'w') as f: f.write(out)
  except:
    print "Unable to save data to send!"
    sys.exit(1)

  # send data with debug
  os.system("{0} -s {1}.{2} -z {3} -i {4} -vv".format(sender,node_name,domain,zbx_server,tmp))
  #os.system("{0} -s {1}.{2} -z {3} -i {4}".format(sender,node_name,domain,zbx_server,tmp))

  os.remove(tmp)

if __name__ == "__main__":
    main(sys.argv[1],sys.argv[2],sys.argv[3])
