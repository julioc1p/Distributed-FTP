from address import NodeKey
from threading import Thread
import Pyro4
import time
import random
import rpyc
import sys
import json
from misc import uhash, send_multicast, recv_multicast

SIZE = 160
NODESIZE = 1 << SIZE
SUCCSSIZE = 3


def repeat_and_sleep(sleep_time):
    def decorator(fun):
        def inner(self, *args, **kwargs):
            while True:
                time.sleep(sleep_time)
                fun(self, *args, **kwargs)
                # if not ret:
                #     return

        return inner

    return decorator


def retry(try_amount):
    def decorator(fun):
        def inner(*args, **kwargs):
            retry_count = 0
            while retry_count < try_amount:
                try:
                    return fun(*args, **kwargs)
                except:
                    retry_count += 1
                    time.sleep(1 << retry_count)
            return

        return inner

    return decorator


class Deamon(Thread):

    def __init__(self, obj, method):
        Thread.__init__(self)
        self.obj_ = obj
        self.method_ = method

    def run(self):
        getattr(self.obj_, self.method_)()


@Pyro4.expose
class Node(object):

    def __init__(self, chord_type,address, other_node=None):
        self.address_ = address
        self.join(other_node)
        self.type = chord_type
        self.successors_ = []


    def address(self):
        return self.address_.ip, self.address_.port

    def __hash__(self):
        return uhash('{}:{}'.format(self.address_.ip, self.address_.port))

    def id(self, n=0):
        return (self.__hash__() + n) % NODESIZE

    def exist(self):
        return True

    def ping(self, addr):
        try:
            return addr().exist()
        except:
            return False

    def modulate(self, id, n=0):
        return (id + n) % NODESIZE

    def inrange(self, id, min, max):
        if min == max:
            return False
        if min < max:
            return min <= id < max
        else:
            return id >= min or id < max

    def start(self):
        Deamon(self, 'start_local_server').start()
        Deamon(self, 'fix_fingers').start()
        Deamon(self, 'stabilize').start()
        Deamon(self, 'update_successors').start()
        Deamon(self, 'replicate').start()
        # Deamon(self, 'clear_replicate').start()

    def get_remote_node(self, address):
        return Pyro4.Proxy(f'PYRO:{address.id}@{address.ip}:{address.port}')

    def start_local_server(self):
        Pyro4.Daemon.serveSimple({self: str(self.__hash__())}, host=self.address_.ip, port=self.address_.port, ns=False)

    def join(self, other_node):
        self.finger_ = [None for _ in range(SIZE)]

        self.predeccessor_ = None

        if other_node:
            remote_node = other_node()
            self.finger_[0] = remote_node.find_successor(self.id())

            succ = self.finger_[0]()    
            pred_id = succ.predeccessor().id
            dht = rpyc.connect(self.address_.ip, self.address_.port+1)
            dht.root.get_keys_from(self.finger_[0], self.modulate(pred_id, 1), self.id())
            dht.close()
        else:
            self.finger_[0] = self.address_
        # self.successors_ = [self.finger_[0]]

    def predeccessor(self):
        if not self.ping(self.predeccessor_):
            return None
        return self.predeccessor_

    def successor(self):
        if self.ping(self.finger_[0]):
            return self.finger_[0]
        for succ in self.successors_:
            if self.ping(succ):
                return succ
        return self.address_

    def find_successor(self, id):

        predec = self.predeccessor()
        if predec and self.inrange(id, self.modulate(predec.id, 1), self.id(1)):
            return self.address_

        node = self.find_predeccessor(id)()
        return node.successor()

    def find_predeccessor(self, id):

        # condicion importante para garantizar que no se quede
        # en un ciclo infinito el while
        if self.id() == self.successor().id:
            return self.address_

        addr = self.address_
        node = addr()
        while not self.inrange(id, self.modulate(node.id(), 1), self.modulate(node.successor().id, 1)):
            addr = node.closest_preceding_finger(id)
            node = addr()
        return addr

    def closest_preceding_finger(self, id):

        for addr in reversed(self.finger_):
            if self.ping(addr) and self.inrange(addr.id, self.id(1), id):
                return addr
        return self.address_

    def verify_keys(self):
        dht = rpyc.connect(self.address_.ip, self.address_.port+1)
        try:
            if self.id() == self.predeccessor().id:
                dht.root.verify_interval(self.id(1), self.id())
            else :
                dht.root.verify_interval(self.modulate(self.predeccessor().id, 1), self.id())
        except:
            pass
        dht.close()

    #pensar en dejar solo discover
    @repeat_and_sleep(1)
    # @retry(3)
    def stabilize(self):
        succ = self.successor()
        x = succ().predeccessor()
        if x and self.inrange(x.id, self.id(1), succ.id):
            self.finger_[0] = x 
            self.successors_ = [self.finger_[0]] + self.successors_[:SUCCSSIZE-1]
        succ().notify(self.address_)
        return True

    def notify(self, other_node):
        if not self.predeccessor() or self.inrange(other_node.id, self.modulate(self.predeccessor().id, 1), self.id()):
            self.predeccessor_ = other_node
            self.verify_keys()

    @repeat_and_sleep(1)
    def fix_fingers(self):
        index = random.randint(0, SIZE - 1)
        self.finger_[index] = self.find_successor(self.id(1 << index))
        return True

    @repeat_and_sleep(1)
    def update_successors(self):
        succ = self.successor()
        if succ.id == self.id():
            return
        self.successors_ = [succ] + succ().successors()

    def successors(self):
        return self.successors_[:SUCCSSIZE - 1]

    @repeat_and_sleep(1)
    def replicate(self):
        dht_addrs = []
        for node in self.successors_:
            if self.ping(node):
                dht_addrs.append(node)
        if len(dht_addrs):
            dht = rpyc.connect(self.address_.ip, self.address_.port+1)
            dht.root.start_backup(dht_addrs)
            dht.close()

    @repeat_and_sleep(30)
    def clear_replicate(self):
        dht = rpyc.connect(self.address_.ip, self.address_.port+1)
        dht.root.clear()
        dht.close()

    @repeat_and_sleep(5)
    def inform(self):
        data = json.dumps({'name':self.type, 'ip':self.address_.ip, 'port':self.address_.port
        , 'id':self.address_.id})
        send_multicast(data)

    @repeat_and_sleep(5)
    def discover(self):
        try:
            data = json.loads(recv_multicast())
            if data['name'] == self.type:
                id = int(data['id'])
                node = NodeKey(data['ip'], int(data['port']))
                if self.inrange(id, self.id(), self.successor().id):
                    self.finger_[0] = node
                    self.successors_ = [self.finger_[0]] + self.successors_[:SUCCSSIZE-1]
        except:
            pass

    def hey(self):
        print('Hey!, I am node {}, my successor is {} and my predeccessor {}'
              .format(self.id(), self.successor().id, self.predeccessor().id if self.predeccessor() else -1))

    def see_fingers(self):
        index = 0
        for finger in self.finger_:
            print('Node {} has successor {}'.format(self.id(1 << index),
                                                    finger.id if finger else finger))
            index += 1

    def see_succs(self):
        r = f'Node {self.id()} has successor list '
        for s in self.successors_:
            r += str(s.id) + ' '
        print(r)


if __name__ == '__main__':
    node = None
    add = NodeKey(sys.argv[1], int(sys.argv[2]))
    if len(sys.argv) == 5 :
        node = Node(add, NodeKey(sys.argv[3], int(sys.argv[4])))
    else :
        node = Node(add)
    node.start()

