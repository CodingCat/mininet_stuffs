__author__ = 'zhunan'

from dctopo import FatTreeTopo
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.node import RemoteController
import sys
import random
import time
from functools import partial

def startnewShuffleJob(maxmapnum, maxreducenum, hosts):
    random.seed(int(round(time.time() * 1000)))
    hostnum = len(hosts)
    mapnum = random.randint(0, maxmapnum)
    reducenum = random.randint(0, maxreducenum)
    mappers = []
    reducers = []
    iperfport = 40000
    for i in range(0, mapnum):
        mappers.append(hosts[random.randint(0, hostnum - 1)])
    for i in range(0, reducenum):
        reducers.append(hosts[random.randint(0, hostnum - 1)])
    for dstHost in reducers:
        dstHost.cmd('iperf -s -p %d &' % iperfport)
        iperfport += 1
    for srcHost in mappers:
        print("d")

def shufflerPerformanceTest(topo, mapnum, reducenum, jobnum, arrivalinterval):

    hosts = topo.layer_nodes(topo.LAYER_HOST)
    net = Mininet(topo=topo, switch=OVSKernelSwitch,
                  controller=partial(RemoteController, ip='127.0.0.1', port='6633'))
    print("net is starting")
    net.start()
    print("net is starting")
    startnewShuffleJob(mapnum, reducenum, hosts)
    net.stop()



if __name__ == '__main__':
    if len(sys.argv) != 7:
        print("usage:shuffle.py podnumber linkspeed(Gbps) "
              "maxmappernum maxreducernum jobnum arrivalInterval")
    print("*** Running Shuffle Performance Test under FatTree\n")
    topo = FatTreeTopo(k=int(sys.argv[1]), speed=int(sys.argv[2]))
    print("*** finish building fat tree topology")
    shufflerPerformanceTest(topo, int(sys.argv[3]), int(sys.argv[4]),
                            int(sys.argv[5]), int(sys.argv[6]))
    print("*** End\n")

