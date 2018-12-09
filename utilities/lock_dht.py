from name_dht import DHTService
import rpyc
from address import NodeKey
from random import randint
from threading import Thread
import time

SIZE = 160
NODE_AMOUNT = 1 << SIZE


def start_dht_service(host):
    PATH = f'{pathlib.Path.home()}/.dftp/locs/'
    dht_server = rpyc.ThreadedServer(lock_dhtService(host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()


def repeat_and_sleep(t):
    def fun(f):
        def wrapper(*args, **kwargs):
            while True:
                time.sleep(t)
                f(*args,**kwargs)
        return wrapper
    return fun

   
class lock_dhtService( DHTService, rpyc.Service):
    def __init__(self, host, DATA, REPL):
        super(DHTService, self).__init__(host, DATA, REPL)


    def exposed_set_key(self, key):
        host = self.chord_node()
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        k = c.root.get_key(key)
        if k == None:
            c.root.set(key, 1)
        c.close()
        Thread(target=clear, args=(self)).start()

    def exposed_remove_key(self, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        l = c.root.get_key(key)
        has = c.root.del_key(key)
        c.close()
        return has

    @repeat_and_sleep(5)
    def clear(self):
        self.save_json(self.hash_table,{})