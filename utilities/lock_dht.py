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

    def exposed_get_keys_from(self, address, min, max):
        connection = rpyc.connect(address.ip, port=address.port+1)
        keys = connection.root.give_key_from(min, max)
        keys = json.loads(keys)
        hash_table = self.open_json(self.hash_table)
        for i in keys:
            self.l.acquire()
            if i not in hash_table:
                hash_table[i] = keys[i]
            self.l.release()
        self.save_json(self.hash_table, keys)

    def exposed_backup(self, data):
        replicate = self.open_json(self.replicate)
        for i in data:
            self.l.acquire()
            if i not in replicate:
                replicate[i] = data[i]
            self.l.release()
        self.save_json(self.replicate, replicate)

    def exposed_get_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        has = c.root.hash_table()
        key = str(key)
        if key in has:
            return None
        key = c.root.set_key(key)
        c.close()
        return key

    def exposed_set(self, key, value):
        hash_table = self.open_json(self.hash_table)
        self.l.acquire()
        key = str(key)
        hash_table[key] = value
        self.l.release()
        self.save_json(self.hash_table, hash_table)
        

    def exposed_set_key(self, key, lock=None):
        host = self.chord_node()
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        if not lock is None:
            k = lock
        else:
            k = randint(1, 1e100)
        c.root.set(key, k)
        c.close()
        return k

    def exposed_remove_key(self, lock, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        if c.root.check_lock(lock, key):
            c.root.del_key(key)
        c.close()

    def exposed_check_lock(self, lock, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        h = c.root.hash_table()
        h = h[key]
        return h == lock

    @repeat_and_sleep(5)
    def clear(self):
        self.save_json(self.hash_table,{})
