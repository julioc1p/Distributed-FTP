from chord import Node
from address import NodeKey
from threading import Thread
import Pyro4
import time
import sys
import rpyc
from dht import start_dht_service
#
# connection = rpyc.connect('localhost', port=23234)
# a = connection.root.get_key(2)
# print(a)
# connection.close()

def convert(classname, dict):
    return NodeKey(dict['ip'], dict['port'])

Pyro4.util.SerializerBase.register_dict_to_class('address.NodeKey', convert)

def create_chord(add, follower):
    node = Node(add, follower)
    node.start()
    while True:
        time.sleep(10)
        node.hey()
        node.see_fingers()
        node.see_succs()


def create_dht(ip, port):
    start_dht_service(NodeKey(ip, int(port)))

def create_dht_chord(ip, port, follower=None):
    td = Thread(target=create_dht, args=(ip, port))
    td.start()
    addc = NodeKey(ip, int(port))
    tc = Thread(target=create_chord, args=(addc, follower))
    tc.start()
    while True:
        time.sleep(10)
        get_backup(ip, port+1)



def set_key(key, value, port):
    con = rpyc.connect('localhost', port=port)
    con.root.set_key(key, value)
    con.close()


def get_key(key, port):
    con = rpyc.connect('localhost', port=port)
    a = con.root.get_key(key)
    # print(a)
    con.close()

def get_backup(ip, port):
    con = rpyc.connect(ip, port=port)
    a = con.root.print_backup()
    con.close()

if __name__ == '__main__':
    p = None
    if len(sys.argv) == 5:
        p = Thread(target=create_dht_chord, args=(sys.argv[1], int(sys.argv[2]), NodeKey(sys.argv[3], int(sys.argv[4]))))
    else:
        p = Thread(target=create_dht_chord, args=(sys.argv[1], int(sys.argv[2])))
    p.start()


# class aService(rpyc.Service):

#     def exposed_p(self):
#         return 'jajaja'

#     def exposed_d(self):
#         c = rpyc.connect('localhost', port=23235)
#         r = c.root.p()
#         print(r)
#         c.close()
    
#     def exposed_print_and_sleep(self):
#         while True:
#             print('jj')
#             time.sleep(5)


# def create():
#     s = rpyc.ThreadedServer(aService(), port=23235)
#     s.start()

# create()
# t = Thread(target=create)
# t.start()
# print('hola')
# c = rpyc.connect('localhost', 23235)
# print('local')
# r = c.root.p()
# print(r)


# def metodo(addr, numero, forward):
#     add = Address('localhost',addr)
#     node = Node(add, numero,forward)
#     node.start()
#     time.sleep(1)
#     while True:
#         time.sleep(20)
#         node.hey()
#         node.see_succs()
#         node.see_fingers()
#
#
# p1 = Process(target=metodo, args=(23234, 6, None))
# p2 = Process(target=metodo, args=(23235, 5, Address('localhost', 23234)))
# p3 = Process(target=metodo, args=(23236, 2, Address('localhost', 23234)))
# p4 = Process(target=metodo, args=(23237, 3, Address('localhost', 23235)))
#
# p1.start()
# time.sleep(2)
#
# p2.start()
# time.sleep(2)
#
# p3.start()
# time.sleep(2)
#
# p4.start()
# time.sleep(23)
# p4.terminate()
# p3.terminate()