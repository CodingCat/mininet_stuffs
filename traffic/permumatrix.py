__author__ = 'zhunan'

import sys
import random
import time
from functools import partial
from datetime import datetime

from mininet.log import lg
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.node import RemoteController
from mininet.link import TCIntf
from mininet.util import custom
from dctopo import FatTreeTopo

globalclientlist = {}

def iperf(hosts, flowsize, outfile):
        """Run iperf between two hosts.
           hosts: list of hosts; if None, uses opposite hosts
           l4Type: string, one of [ TCP, UDP ]
           returns: results two-element array of server and client speeds"""
        client, server = hosts
        print( '*** Iperf: testing bandwidth between ' )
        print( "%s and %s\n" % ( client.name, server.name ) )
        iperfArgs = 'iperf '
        bwArgs = ''
        server.cmd(iperfArgs + '-s &', printPid=True)
        while 'Connected' not in client.cmd(
                        'sh -c "echo A | telnet -e A %s 5001"' % server.IP()):
            print('waiting for iperf to start up...')
            time.sleep(.5)
        client.cmd(iperfArgs + '-n ' + str(flowsize) + 'MB -c ' + server.IP() + ' ' + \
                             bwArgs + " >> " + outfile + "&", printPid=True)
        if not globalclientlist.has_key(client):
            globalclientlist[client] = []
        globalclientlist[client].append(client.lastPid)
        return

def permuMatrix(net, fattreetopo, flowsize):
    selectedhost = []
    hosts = fattreetopo.layer_nodes(fattreetopo.LAYER_HOST)
    outfile = "permumatrix_" + str(round(time.time()))
    for host in hosts:
        receivedidx = random.randint(0, len(hosts) - 1)
        while receivedidx in selectedhost or \
                        hosts[receivedidx] == host:
            receivedidx = random.randint(0, len(hosts) - 1)
        selectedhost.append(receivedidx)
        iperf([net.get(host), net.get(hosts[receivedidx])],
              flowsize=flowsize,
              outfile=outfile)


def waitForAll():
    for host, pidlist in globalclientlist.iteritems():
        for pid in pidlist:
            host.cmd('wait', pid)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("usage:shuffle.py podnumber linkspeed(Gbps) flowsize")
        sys.exit(0)
    startTime = datetime.now()
    lg.setLogLevel('info')
    print("*** Running Permutation Matrix Performance Test under FatTree\n")
    topo = FatTreeTopo(k=int(sys.argv[1]), speed=int(sys.argv[2]))
    print("*** finish building fat tree topology")
    intf = custom( TCIntf, bw=int(sys.argv[2]))
    net = Mininet(topo=topo, intf=intf, switch=OVSKernelSwitch,
                  controller=partial(RemoteController, ip='127.0.0.1', port=6633))
    print("net is starting")
    lg.setLogLevel('info')
    net.start()
    permuMatrix(net, topo, int(sys.argv[3]))
    waitForAll()
    net.stop()
    print("time cost:" + str(datetime.now() - startTime))
    print("*** End\n")
