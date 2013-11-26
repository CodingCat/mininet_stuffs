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
import re

globalserverlist = []
globalclientlist = {}

def parseIperf( iperfOutput ):
        """Parse iperf output and return bandwidth.
           iperfOutput: string
           returns: result string"""
        r = r'([\d\.]+ \w+/sec)'
        m = re.findall( r, iperfOutput )
        if m:
            return m[-1]
        else:
            # was: raise Exception(...)
            lg.error( 'could not parse iperf output: ' + iperfOutput )
            return ''

def iperf(hosts, flowsize, outfile):
        """Run iperf between two hosts.
           hosts: list of hosts; if None, uses opposite hosts
           l4Type: string, one of [ TCP, UDP ]
           returns: results two-element array of server and client speeds"""
       # if not hosts:
       #     hosts = [ self.hosts[ 0 ], self.hosts[ -1 ] ]
       # else:
       #     assert len( hosts ) == 2
        client, server = hosts
        print( '*** Iperf: testing bandwidth between ' )
        print( "%s and %s\n" % ( client.name, server.name ) )
        #server.cmd( 'killall -9 iperf' )
        iperfArgs = 'iperf '
        bwArgs = ''
        if server not in globalserverlist:
            #server.sendCmd( iperfArgs + '-s ', printPid=True )
            server.cmd(iperfArgs + '-s &', printPid=True)
            globalserverlist.append(server)
        servout = ''
        #while server.lastPid is None:
         #   servout += server.monitor()
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

def startnewShuffleJob(net, maxmapnum, maxreducenum, hosts, outfile):

    random.seed(int(round(time.time() * 1000)))
    hostnum = len(hosts)
    mapnum = random.randint(1, maxmapnum)
    reducenum = random.randint(1, maxreducenum)
    mappers = []
    reducers = []
    selectedMappers = []
    selectedReducers = []
    for i in range(0, mapnum):
        a = random.randint(0, hostnum - 1)
        while a in selectedMappers:
            a = random.randint(0, hostnum - 1)
        mappers.append(hosts[a])
        selectedMappers.append(a)
    for i in range(0, reducenum):
        a = random.randint(0, hostnum - 1)
        while a in selectedReducers or a in selectedMappers:
            a = random.randint(0, hostnum - 1)
        reducers.append(hosts[a])
        selectedReducers.append(a)
    line = ''
    for srcHost in mappers:
        for dstHost in reducers:
            iperf(hosts=[net.get(srcHost), net.get(dstHost)], flowsize=500, outfile=outfile)
    print line


def shufflerPerformanceTest(topo, linkspeed, mapnum, reducenum, jobnum, arrivalinterval):

    hosts = topo.layer_nodes(topo.LAYER_HOST)
    intf = custom( TCIntf, bw=linkspeed )
    net = Mininet(topo=topo, intf=intf, switch=OVSKernelSwitch,
                  controller=partial(RemoteController, ip='127.0.0.1', port=6633))
    print("net is starting")
    net.start()
    outfile = "shuffle_" + str(round(time.time()))
    for i in range(0, jobnum):
        print "start a new shuffle job"
        startnewShuffleJob(net, mapnum, reducenum, hosts, outfile)
        time.sleep(arrivalinterval)
    waitForAll()
    net.stop()

def waitForAll():
    for host, pidlist in globalclientlist.iteritems():
        for pid in pidlist:
            host.cmd('wait', pid)

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

