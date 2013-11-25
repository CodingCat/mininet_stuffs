__author__ = 'zhunan'

from dctopo import FatTreeTopo
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.node import RemoteController
import sys
import random
import time
from functools import partial

def shufflerPerformanceTest(topo, mapnum, reducenum):

    hosts = topo.layer_nodes(topo.LAYER_HOST)
    net = Mininet(topo=topo, switch=OVSKernelSwitch, controller=partial(RemoteController, ip='127.0.0.1', port='6633')
    print "net is starting"
    net.start()
    print "net is starting"
    random.seed(int(round(time.time() * 1000)))
    hostnum = len(hosts)
    mappers = []
    reducers = []
    print "mapnum:%d" % mapnum
    for i in range(0, mapnum):
        mappers.append(hosts[random.randint(0, hostnum - 1)])
    print "reducenum:%d" % reducenum
    for i in range(0, reducenum):
        reducers.append(hosts[random.randint(0, hostnum - 1)])

    net.stop()



if __name__ == '__main__':
    print "*** Running Shuffle Performance Test under FatTree\n",
    topo = FatTreeTopo(k=int(sys.argv[1]), speed=int(sys.argv[2]))
    print "*** finish building fat tree topology"
    shufflerPerformanceTest(topo, 8, 8)
    print "*** End\n"

