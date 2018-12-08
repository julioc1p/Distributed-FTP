from multiprocessing import Process
from chord import Node

import time

def m(port, id, parent):
    ip, port = ('127.0.0.1', port)
    n = Node(ip, port, id, parent)
    n.start()

p1 = Process(target=m, args=(9000, 0, None))
p2 = Process(target=m, args=(9001, 1, None))
p3 = Process(target=m, args=(9002, 3, None))




p1.start()
time.sleep(2)
p2.start()
time.sleep(2)
p3.start()

n1 = get_remote_node('127.0.0.1', "9000")
n2 = get_remote_node('127.0.0.1', "9001")
n3 = get_remote_node('127.0.0.1', "9002")

n1.join(None)

time.sleep(3)

n2.join(('127.0.0.1', "9000"))
print "join n1 con n2"
time.sleep(3)


n3.join(('127.0.0.1', "9000"))
print "join n1 con n3"
time.sleep(3)

    #TEST---DHT-------

time.sleep(15)
#
print n1.Show_FT()
print n2.Show_FT()
print n3.Show_FT()

time.sleep(2)
#
n1.Add_Key(0, "hola")
n1.Add_Key(4, "loca")
n1.Add_Key(60, "lale")
n1.Add_Key(45, "shei")
n1.Add_Key(333, "paapa")


time.sleep(2)
print n1.Exist_Key(3)

print n1.Exist_Key(4)

print n1.Exist_Key(0)

print n1.Get_Value(4)


time.sleep(3)

print n1.list_of_keys()
print n2.list_of_keys()
print n3.list_of_keys()

time.sleep(8)

print n2.Get_Value(333)