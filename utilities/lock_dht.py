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

FLAG_R = 1
FLAG_W = 2
FLAG_D = 3

def create_dht(ip, port):
    host = address.NodeKey(ip, port)
    PATH = f'{pathlib.Path.home()}/.dftp/locks/'
    dht_server = rpyc.ThreadedServer(lock_dhtService('lock', host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()


def create_chord(name, host, follower):
    print(name, '\t\t', host, '\t\t', follower)
    node = Node(name, host, follower)
    node.start()
    # while 1:
    #     time.sleep(5)
    #     node.hey()


def start_lock_service(ip, port, follower_ip=None, follower_port=None):

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
    tc = Thread(target=create_chord, args=('lock_chord', host, follower))
    tc.start()

def repeat_and_sleep(t):
    def fun(f):
        def wrapper(*args, **kwargs):
            while True:
                time.sleep(t)
                f(*args,**kwargs)
        return wrapper
    return fun

   
class lock_dhtService( DHTService, rpyc.Service):
    def __init__(self, name, host, PATH):
        DHTService.__init__(self, name, host, PATH)
        # Thread(target=self.clear).start()

    def exposed_lock(self, key, flag):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        value = c.root.get(key)
        key = str(key)
        if not value:
            c.root.set(key, flag)
            c.close()
            return True
        c.close()
        if FLAG_R == flag == value:
            return True
        return False

    def exposed_remove_lock(self, key, flag):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        if c.root.check_lock(flag, key):
            c.root.remove(key)
        c.close()

    def exposed_check_lock(self, lock, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        h = c.root.get(key)
        return h == lock

    #consultar
    @repeat_and_sleep(5)
    def clear(self):
        self.save_json(self.hash_table,{})
