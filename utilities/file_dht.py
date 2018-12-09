from name_dht import DHTService
import rpyc
from address import NodeKey
from random import randint

SIZE = 160
NODE_AMOUNT = 1 << SIZE


def start_dht_service(host):
    PATH = f'{pathlib.Path.home()}/.dftp/minions/'
    dht_server = rpyc.ThreadedServer(
        file_dhtService(host, PATH), port=host.port + 1)
    print('\n', dht_server, '\n')
    dht_server.start()

def verify_lock(mode):
    def fun(f)
        def wrapper(self, lock, key, *args, **kawarg):
            lock = self.get_lock()
            lock = rpyc.connect(lock[0], lock[1])
            l = lock.root.get_key('.'.joint(key.split('.')[:-1]))
            if not l and mode:
                lock.close()
                return None
            r = f(self, lock, key, *args, **kwargs)
            lock.root.remove_key(lock, key)
            return f
        return wrapper
    return  fun
        

class file_dhtService(DHTService, rpyc.Service):

    def __init__(self, host, DATA, REPL):
        super(DHTService, self).__init__(host, DATA, REPL)

    def get_lock(self, string):
        pass

    @verify_lock
    def exposed_set_key(self, key, data):
        return super(DHTService, self).exposed_set_key(data)

    @verify_lock
    def  exposed_get_key(self, key)
        return super(DHTService, self).exposed_get_key(data)

    @verify_lock
    def exposed_del_key(self, key)
        return super(DHTService, self).exposed_del_key(data)