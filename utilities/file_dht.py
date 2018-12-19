from name_dht import DHTService
from random import randint
from chord import Node
import rpyc
import json
import address
from misc import uhash, recv_multicast, send_multicast
from threading import Thread, Lock
import sys
import pathlib
import os
from chord import Node
import time
from config import *
import Pyro4

def convert(classname, dict):
    return address.NodeKey(dict['ip'], dict['port'])


Pyro4.util.SerializerBase.register_dict_to_class('address.NodeKey', convert)


def create_dht(ip, port):
    host = address.NodeKey(ip, port)
    PATH = f'{pathlib.Path.home()}/.dftp/names/'
    dht_server = rpyc.ThreadedServer(DHTService(
        'name', host, PATH), port=host.port + 1)
    print('\n', uhash(f'{ip}:{port+1}'), dht_server, '\n')
    dht_server.start()


def create_chord(name, host, follower):
    print(name, '\t\t', host, '\t\t', follower)
    node = Node(name, host, follower)
    node.start()


def start_file_service(ip, port, follower_ip=None, follower_port=None):
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



class file_dhtService(DHTService, rpyc.Service):

    def __init__(self, name, host, PATH):
        DHTService.__init__(self, name, host, PATH)
        self.path = PATH
        self.data_path = PATH + str(self.hash) + 'data/'
        self.backup_path = PATH + str(self.hash) + 'backup/'
        self.init_paths()


    def init_paths(self):
        try:
            os.mkdir(self.path)
        except:
            pass
        try:
            os.mkdir(self.data_path)
        except:
            pass
        try:
            os.mkdir(self.backup_path)
        except:
            pass

    def delete_data(self, key):
        name = str(uhash(self.hash_table[key][1])) + '.jjc'
        if not os.path.exists(self.data_path + name):
            return False
        os.remove(self.data_path + name)
        return True

    def delete_backup(self, key):
        name = str(uhash(self.replicate[key][1])) + '.jjc'
        if not os.path.exists(self.backup_path + name):
            return False
        os.remove(self.backup_path + name)
        return True

    def set_data(self, key, data):
        # self.l.acquire()
        path = self.data_path + str(uhash(key)) + '.jjc'
        fs = open(path, 'wb')
        fs.write(data)
        fs.close()
        # self.l.release()
        return True

    def get_data(self, key):
        if not key in self.hash_table:
            return None
        # self.l.acquire()
        path = self.data_path + str(uhash(self.hash_table[key][1])) + '.jjc'
        fs = open(path, 'rb')
        data = fs.read()
        fs.close()
        # self.l.release()
        return data

    def set_backup(self, key, data):
        # self.l.acquire()
        path = self.backup_path + str(uhash(key)) + '.jjc'
        fs = open(path, 'wb')
        fs.write(data)
        fs.close()
        # self.l.release()
        return True

    def get_backup(self, key):
        if not key in self.replicate:
            return None
        # self.l.acquire()
        path = self.data_path + str(uhash(self.replicate[key][1])) + '.jjc'
        fs = open(path, 'rb')
        data = fs.read()
        fs.close()
        # self.l.release()
        return data

    def exposed_get_data(self, key):
        return self.get_data(key)

    def exposed_verify_interval(self, min, max):
        aux = {}
        for i in self.hash_table:
            if not self.inrange(uhash(i), min, max + 1):
                aux[i] = self.hash_table[i]
                self.delete_data(i)
        for i in self.replicate:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = self.replicate[i]
                self.delete_backup(i)
        self.hash_table = aux
        for i in self.hash_table:
            if i in self.replicate:
                self.replicate.pop(i)

    def exposed_give_key_from(self, min, max):
        aux = {}
        for i in self.hash_table:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = self.hash_table[i]
        for i in aux:
            yield (i, aux[i][0])
        #esta ultima iteracion es para removerlos de data
        for i in aux:
            self.delete_data(i)
            self.hash_table.pop(i)

    def exposed_get_keys_from(self, address, min, max):
        connection = rpyc.connect(address.ip, port=address.port+1)
        for item in connection.root.give_key_from(min, max):
            i = item[0]
            if i not in self.hash_table or self.hash_table[i][0] < int(item[1]):
                data = connection.root.get_data(i)
                self.hash_table[i] = (item[1], i)
                self.set_data(i, data)

    def exposed_backup(self, addr, data):
        c = rpyc.connect(addr[0], addr[1])
        for i in data:
            if i not in self.replicate or self.replicate[i][0] < int(data[i][0]):
                self.replicate[i] = data[i]
                data_ = c.root.get_data(i)
                self.set_backup(i, data_)
        c.close()
                

    def exposed_get(self, key):
        if not key in self.hash_table:
            return None
        return (self.hash_table[key][0], self.get_data(key))

    def exposed_set(self, key, value):
        key = str(key)
        if key not in self.hash_table:
            self.hash_table[key] = (1, key)
        else:
            self.hash_table[key] = (self.hash_table[key][0] + 1, key)
        self.set_data(key, value)

    def exposed_remove(self, key):
        if key not in self.hash_table:
            return None
        data = self.get_data(key)
        self.delete_data(key)
        r = self.hash_table[key]
        return (r[0], data)

if __name__ == "__main__":
    if len(sys.argv) > 3:
        start_file_service(sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    else:
        start_file_service(sys.argv[1], int(sys.argv[2]))
