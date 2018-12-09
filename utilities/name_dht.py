import rpyc, json
import address as address
from misc import uhash, recv_multicast, send_multicast
from threading import Thread, Lock
import sys, pathlib, os
from chord import Node
import time

SIZE = 160
NODE_AMOUNT = 1 << SIZE


def recv_MC(name):
    i = 10
    while i:
        try:
            data = recv_multicast()
            print(data)
            data = json.loads(data)
            if(data['name'] == f'{name}_chord'):
                return data
        except:
            return
    return None

def create_dht(ip, port):
    host = address.NodeKey(ip, port)
    PATH = f'{pathlib.Path.home()}/.dftp/names/'
    dht_server = rpyc.ThreadedServer(DHTService(
        'name', host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()

def create_chord(name, host,follower): 
    print(name, '\t\t', host, '\t\t', follower)
    node = Node(name, host, follower)
    node.start()
    while 1:
        time.sleep(5)
        node.hey()

def start_name_service(ip, port, follower_ip=None, follower_port=None):

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
        hash = uhash(f'{host.ip}:{host.port + 1}')
        DATA += str(hash) + '_data.json'
        REPL += str(hash) + '_repl.json'
        self.chord_node = host
        self.l = Lock()
        self.path = PATH
        self.hash_table = DATA
        self.replicate = REPL
        Thread(target=self.send_MC).start()

    @repeat_and_sleep(5)
    def send_MC(self):
        data = {'name': f'{self.name}_dht',
                'ip': self.chord_node.ip,
                'port': str(self.chord_node.port + 1)
                }
        send_multicast(json.dumps(data))

    def launch_json(self):
        try:
            os.mkdir(f'{pathlib.Path.home()}/.dftp/')
        except:
            pass
        try:
            os.mkdir(self.path)
        except:
            pass
        try:
            f = open(self.hash_table, 'r')
            f.close()
        except:
            f = open(self.hash_table, 'w')
            json.dump({}, f)
            f.close()
        try:
            f = open(self.replicate, 'r')
            f.close()
        except:
            f = open(self.replicate, 'w')
            json.dump({}, f)
            f.close()

    def open_json(self, dir):
        self.launch_json()
        self.l.acquire()
        f = open(dir, 'r')
        aux = json.load(f)
        f.close()
        self.l.release()
        return aux

    def save_json(self, dir, data):
        self.launch_json()
        self.l.acquire()
        f = open(dir, 'w')
        json.dump(data, f)
        f.close()
        self.l.release()

    def exposed_verify_interval(self, min, max):
        aux = {}
        hash_table = self.open_json(self.hash_table)
        replicate = self.open_json(self.replicate)
        for i in hash_table:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = hash_table[i]
        for i in replicate:
            if self.inrange(uhash(i), min, max + 1) and i not in aux:
                aux[i] = replicate[i]
        for i in aux:
            if i in replicate:
                replicate.pop(i)
        self.save_json(self.hash_table, aux)
        self.save_json(self.replicate, replicate)

    # def exposed_clear(self):
    #     f = open(self.replicate, 'w')
    #     json.dump({}, f)
    #     f.close()

    def exposed_give_key_from(self, min, max):
        aux = {}
        hash_table = self.open_json(self.hash_table)
        for i in hash_table:
            if self.inrange(uhash(i), min, max + 1):
                aux[i] = hash_table[i]
        for i in aux:
            hash_table.pop(i)
        r = json.dumps(aux)
        return r

    def exposed_get_keys_from(self, address, min, max):
        connection = rpyc.connect(address.ip, port=address.port+1)
        keys = connection.root.give_key_from(min, max)
        keys = json.loads(keys)
        hash_table = self.open_json(self.hash_table)
        for i in keys:
            self.l.acquire()
            if i not in hash_table or int(hash_table[i][0]) > keys[i][0]:
                hash_table[i] = keys[i]
            self.l.release()
        self.save_json(self.hash_table, keys)

    def exposed_backup(self, data):
        replicate = self.open_json(self.replicate)
        for i in data:
            self.l.acquire()
            if i not in replicate or int(replicate[i][0]) < int(data[i][0]):
                replicate[i] = data[i]
            self.l.release()
        self.save_json(self.replicate, replicate)

    def exposed_start_backup(self, dhts):
        for i in dhts:
            if i.ip == self.chord_node.ip and i.port + 1 == self.chord_node.port + 1:
                return
            c = rpyc.connect(i.ip, i.port+1)
            hash_table = self.open_json(self.hash_table)
            t = hash_table.copy()
            c.root.backup(t)
            c.close()

    def exposed_hash_table(self):
        hash_table = self.open_json(self.hash_table)
        return hash_table

    def exposed_set(self, key, value):
        hash_table = self.open_json(self.hash_table)
        self.l.acquire()
        key = str(key)
        if key not in hash_table:
            hash_table[key] = (1, value)
        else:
            hash_table[key] = (hash_table[key][0] + 1, value)
        self.l.release()
        self.save_json(self.hash_table, hash_table)

    def exposed_get_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        has = c.root.hash_table()
        key = str(key)
        if key not in has:
            return None
        r = tuple(has[key])
        c.close()
        return r

    def exposed_set_key(self, key, value):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        c.root.set(key, value)
        c.close()

    def exposed_del_key(self, key):
        h = self.open_json(self.hash_table)
        if key not in h:
            return None
        r = h.pop(key)
        self.save_json(self.hash_table,h)
        return r

    def exposed_remove_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        has = c.root.del_key(key)
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
    start_dht_service(sys.argv[1], int(sys.argv[2]))