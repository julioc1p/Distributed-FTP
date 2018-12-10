from name_dht import DHTService
from random import randint
from chord import Node
import rpyc
import json
import address as address
from misc import uhash, recv_multicast, send_multicast
from threading import Thread, Lock
import sys
import pathlib
import os
from chord import Node
import time

SIZE = 160
NODE_AMOUNT = 1 << SIZE


def create_dht(ip, port):
    host = address.NodeKey(ip, port)
    PATH = f'{pathlib.Path.home()}/.dftp/files/'
    dht_server = rpyc.ThreadedServer(file_dhtService('file', host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()


def create_chord(name, host, follower):
    print(name, '\t\t', host, '\t\t', follower)
    node = Node(name, host, follower)
    node.start()
    # while 1:
    #     time.sleep(5)
    #     node.hey()


def start_file_service(ip, port, follower_ip=None, follower_port=None):

    t = Thread(target=create_dht, args=(ip, port))
    t.start()
    time.sleep(2)
    host = address.NodeKey(ip, int(port))
    if follower_ip is None:
        follower = None
    else:
        follower = address.NodeKey(follower_ip, follower_port)
    # follower = recv_MC('name')
    # if not follower is None:
    #     follower = address.NodeKey(follower['ip'], follower['port'])
    tc = Thread(target=create_chord, args=('file_chord', host, follower))
    tc.start()


class file_dhtService(DHTService, rpyc.Service):

    def __init__(self, name, host, PATH):
        DHTService.__init__(self, name, host, PATH)

    def exposed_set_key(self, key, value):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        c.root.set(key, value)
        c.close()

    # def exposed_remove_key(self, key):
    #     c = self.chord_node()
    #     h = c.find_successor(uhash(key))
    #     c = rpyc.connect(h.ip, port=h.port + 1)
    #     has = c.root.del_key(key)
    #     c.close()
    #     return has
