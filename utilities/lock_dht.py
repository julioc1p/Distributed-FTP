from name_dht import DHTService
import rpyc
from address import NodeKey
from random import randint

SIZE = 160
NODE_AMOUNT = 1 << SIZE


def start_dht_service(host):
    PATH = f'{pathlib.Path.home()}/.dftp/locs/'
    dht_server = rpyc.ThreadedServer(lock_dhtService(host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()

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
            c.root.set(key, randint(1, 1e100))
        c.close()

    def exposed_remove_key(self, lock, key):
        c = self.chord_node()
        h = c.find_successor(uhash(key))
        c = rpyc.connect(h.ip, port=h.port + 1)
        l = c.root.get_key(key)
        if lock != l:
            return None
        has = c.root.del_key(key)
        c.close()
        return has
