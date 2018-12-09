import hashlib
import struct
import sys

def send_multicast(data):
    message = data
    multicast_group = ('224.3.29.71', 10000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.2)
    ttl = struct.pack('b', 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    try:
        sent = sock.sendto(message, multicast_group)
    finally:
        sock.close()

def recv_multicast():
    multicast_group = '224.3.29.71'
    server_address = ('', 10000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)
    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_ADD_MEMBERSHIP,
        mreq)
    try:
        data, address = sock.recvfrom(1024)
        return data

SIZE = 1 << 160


def uhash(data):
    h = hashlib.sha1()
    s = str(data)
    h.update(s.encode())
    ident = int.from_bytes(h.digest(), byteorder='little') % SIZE
    return ident


def ping(addr):
    try:
        return addr().exist()
    except:
        return False


