import hashlib
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


