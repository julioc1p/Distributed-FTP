import rpyc, json
import address as address
from misc import uhash, recv_multicast, send_multicast
from threading import Thread, Lock
import sys, pathlib, os
from chord import Node, Deamon
import time
import pickle
from config import *

def recv_MC(name):
    i = 10
    while i:
        try:
            data = recv_multicast()
            data = json.loads(data)
            if(data['name'] == f'{name}_chord'):
                return data
        except:
            return
    return None

def exposed_get_succ(self):
    host =  self.chord_node().successor()
    ip, port = host.ip, host.port
    return ip,port

def create_dht(ip, port):
    host = address.NodeKey(ip, port)
    PATH = f'{pathlib.Path.home()}/.dftp/names/'
    dht_server = rpyc.ThreadedServer(DHTService(
        'name', host, PATH), port=host.port + 1)
    print('\n', uhash(f'{ip}:{port+1}') , dht_server, '\n')
    dht_server.start()

def create_chord(name, host,follower): 
    print(name, '\t\t', host, '\t\t', follower)
    node = Node(name, host, follower)
    node.start()

def start_name_service(ip, port, follower_ip=None, follower_port=None):
    t = Thread(target=create_dht, args=(ip, port))
    t.start()
    time.sleep(2)
    host = address.NodeKey(ip, int(port))
    if follower_ip is None:
        follower = None
    else:
        follower = address.NodeKey(follower_ip, follower_port)
    tc = Thread(target=create_chord, args=('name_chord', host, follower))
    tc.start()

def repeat_and_sleep(i):
    def func(f):
        def wrapper(*args, **kargs):
            while True:
                time.sleep(i)
                f(*args, **kargs)
        return wrapper
    return func


class DHTService(rpyc.Service):

    def __init__(self, name, host, PATH):
        self.name = name
        DATA = PATH
        REPL = PATH
        self.hash = uhash(f'{host.ip}:{host.port + 1}')
        DATA += str(hash) + '_data.json'
        REPL += str(hash) + '_repl.json'
        self.chord_node = host
        self.l = Lock()
        self.path = PATH
        self.hash_table = {}
        self.replicate = {}
        Deamon(self, 'clear_backup').start()

    def exposed_verify_interval(self, min, max):
        aux = {}
        for i in self.hash_table:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = self.hash_table[i]
        for i in self.replicate:
            if self.inrange(uhash(i), min, max + 1) and i not in aux:
                aux[i] = self.replicate[i]
        self.hash_table = aux
        for i in self.hash_table:
            if i in self.replicate:
                self.replicate.pop(i)

    @repeat_and_sleep(600)
    def clear_backup(self):
        self.replicate = {}

    def exposed_give_key_from(self, min, max):
        aux = {}
        for i in self.hash_table:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = self.hash_table[i]
        for i in aux:
            self.hash_table.pop(i)
        return pickle.dumps(aux)

    def exposed_get_keys_from(self, address, min, max):
        connection = rpyc.connect(address.ip, port=address.port+1)
        keys = connection.root.give_key_from(min, max)
        keys = pickle.loads(keys)
        for i in keys:
            if i not in self.hash_table or self.hash_table[i][0] < int(keys[i][0]):
                self.hash_table[i] = keys[i]

    def exposed_backup(self, addr, data):
        for i in data:
            if i not in self.replicate or self.replicate[i][0] < int(data[i][0]):
                self.replicate[i] = data[i]

    def exposed_start_backup(self, dhts):
        addr = (self.chord_node.ip, self.chord_node.port+1)
        for i in dhts:
            if i.ip == self.chord_node.ip and i.port + 1 == self.chord_node.port + 1:
                return
            c = rpyc.connect(i.ip, i.port+1)
            t = self.hash_table.copy()
            c.root.backup(addr, t)
            c.close()

    def exposed_hash_table(self):
        hash_table = self.open_json(self.hash_table)
        return hash_table

    def exposed_get(self, key):
        key = str(key)
        if not key in self.hash_table:
            return None
        return self.hash_table[key]

    def exposed_set(self, key, value):
        key = str(key)
        if key not in self.hash_table:
            self.hash_table[key] = (1, value)
        else:
            self.hash_table[key] = (self.hash_table[key][0] + 1, value)

    def exposed_get_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        value = c.root.get(key)
        c.close()
        return value

    def exposed_set_key(self, key, value):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        c.root.set(key, value)
        c.close()

    def exposed_remove(self, key):
        if key not in self.hash_table:
            return None
        r = self.hash_table.pop(key)
        return r

    def exposed_remove_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        has = c.root.remove(key)
        c.close()
        return has

    def exposed_alive(self):
        return True

    def exposed_print_backup(self):
        print(self.chord_node.id, self.hash_table, self.replicate)

    def inrange(self, id, min, max):
        if min < max:
            return min <= id < max
        else:
            return id >= min or id < max


if __name__ == '__main__':
    if len(sys.argv) > 3:
        start_name_service(sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    else:
        start_name_service(sys.argv[1], int(sys.argv[2]))
