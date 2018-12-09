from chord import Node
from address import NodeKey
from threading import Thread
import Pyro4
import time
import rpyc
from name_dht import start_name_service


def convert(classname, dict):
    return NodeKey(dict['ip'], dict['port'])


Pyro4.util.SerializerBase.register_dict_to_class('address.NodeKey', convert)

def create_dht_chord(ip, port):
    start_name_service(ip, port)


def set_key(key, value, port):
    con = rpyc.connect('localhost', port=port)
    con.root.set_key(key, value)
    con.close()


def get_key(key, port):
    con = rpyc.connect('localhost', port=port)
    a = con.root.get_key(key)
    print(a)
    con.close()


def get_backup(port):
    con = rpyc.connect('localhost', port=port)
    con.root.print_backup()
    con.close()


p1 = Thread(target=create_dht_chord, args=('192.168.1.132', 23234))
p2 = Thread(target=create_dht_chord, args=('192.168.1.132', 23236))
p3 = Thread(target=create_dht_chord, args=('192.168.1.132', 23238))

p1.start()
time.sleep(5)


# get_key(6, 23235)
# get_key(10, 23235)
# get_key(11, 23235)
# get_key(13, 23235)

p2.start()
time.sleep(3)
p3.start()

time.sleep(3)

# get_key(6, 23235)
# get_key(15, 23235)
# get_key(14, 23235)
# get_key(139, 23235)

# time.sleep(10)
# get_backup(23235)
# get_backup(23237)
# get_backup(23239)


# address = NodeKey('localhost', 23234)
# c = Pyro4.Proxy(f'PYRO:{(uhash("{}:{}".format(address.ip, address.port)))}@{address.ip}:{address.port}')
# a1=c.find_successor(6).info()
# a2=c.find_successor(10).info()
# a3=c.find_successor(11).info()
# a4=c.find_successor(13).info()
# print(a1,a2,a3,a4)


# connection = rpyc.connect('localhost', port=23234)
# a = connection.root.set_key(1,)
# print(a)
# connection.close()

# d = {1: (1, 2, 3),
#      2: (1, 2, 3),
#      3: (1, 2, 3),
#      4: (1, 2, 3),
#      5: (1, 2, 3),
#      6: (1, 2, 3),
#      7: (1, 2, 3)
#      }
#
# a={}
#
# for i in d:
#     print(i)
#     a[i] = d[i]
#
#
# print(a)

# class aService(rpyc.Service):
#
#     def exposed_p(self):
#         return 'jajaja'
#
#     def exposed_d(self):
#         c = rpyc.connect('localhost', port=23235)
#         r = c.root.p()
#         print(r)
#         c.close()
#
#
# def create():
#     s = rpyc.ThreadedServer(aService(), port=23235)
#     s.start()
#
#
# t = Thread(target=create)
# t.start()
# print('hola')
# c = rpyc.connect('localhost', 23235)
# print('local')
# r = c.root.p()
# print(r)


# def metodo(addr, numero, forward):
#     add = NodeKey('localhost',addr)
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
# p2 = Process(target=metodo, args=(23235, 5, NodeKey('localhost', 23234)))
# p3 = Process(target=metodo, args=(23236, 2, NodeKey('localhost', 23234)))
# p4 = Process(target=metodo, args=(23237, 3, NodeKey('localhost', 23235)))
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

