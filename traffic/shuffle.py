__author__ = 'zhunan'

from dctopo import FatTreeTopo
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.node import RemoteController
from mininet.log import lg
from mininet.link import TCIntf
from mininet.util import custom
import sys
import random
import time
from functools import partial

def startnewShuffleJob(net, maxmapnum, maxreducenum, hosts):
    random.seed(int(round(time.time() * 1000)))
    hostnum = len(hosts)
    mapnum = random.randint(1, maxmapnum)
    reducenum = random.randint(1, maxreducenum)
    mappers = []
    reducers = []
    iperfport = 40000
    listeningports = []
    for i in range(0, mapnum):
        mappers.append(hosts[random.randint(0, hostnum - 1)])
    for i in range(0, reducenum):
        reducers.append(hosts[random.randint(0, hostnum - 1)])
    for dstHost in reducers:
        net.get(dstHost).sendCmd('iperf -s -D -p %d ' % int(iperfport))
        iperfport += 1
        listeningports.append(iperfport - 1)
    for srcHost in mappers:
        for c in range(0, len(listeningports)):
            print("testing iperf")
            net.get(srcHost).sendCmd('iperf -n 500MB -c 127.0.0.1 -p %d ' % listeningports[c])
            line = net.get(srcHost).waitOutput()
            print(line)


def shufflerPerformanceTest(topo, linkspeed, mapnum, reducenum, jobnum, arrivalinterval):

    hosts = topo.layer_nodes(topo.LAYER_HOST)
    intf = custom( TCIntf, bw=linkspeed )
    net = Mininet(topo=topo, intf=intf, switch=OVSKernelSwitch,
                  controller=partial(RemoteController, ip='127.0.0.1', port=6633))
    print("net is starting")
    net.start()
    startnewShuffleJob(net, mapnum, reducenum, hosts)
   # time.sleep(60)
    net.stop()



if __name__ == '__main__':
    if len(sys.argv) != 7:
        print("usage:shuffle.py podnumber linkspeed(Gbps) "
              "maxmappernum maxreducernum jobnum arrivalInterval")
        sys.exit(0)
    lg.setLogLevel('info')
    print("*** Running Shuffle Performance Test under FatTree\n")
    topo = FatTreeTopo(k=int(sys.argv[1]), speed=int(sys.argv[2]))
    print("*** finish building fat tree topology")
    shufflerPerformanceTest(topo, float(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]),
                            int(sys.argv[5]), int(sys.argv[6]))
    print("*** End\n")

