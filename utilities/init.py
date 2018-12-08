import rpyc
from dht import start_dht_service
from chord import Node
from address import Address
from threading import Thread
import sys

def create_chord(add, id):
    node = Node(add, id)
    node.start()


def create_dht(add, lower, upper):
    start_dht_service(lower, upper, add)


def create_dht_chord(ip, port, id, lower, upper):
    addc = Address(ip, int(port))
    addd = Address(ip, int(port))
    lower = int(lower)
    upper = int(upper)
    id = int(id)
    tc = Thread(create_chord(addc, id))
    tc.start()
    td = Thread(create_dht(addd, lower, upper))
    td.start()


create_dht_chord(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
