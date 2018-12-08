from misc import uhash
import Pyro4

class NodeKey(object):

    def __init__(self, ip, port):
        self.exposed_ip = ip
        self.exposed_port = port 
        self.ip = ip
        self.port = int(port)
        self.id = uhash('{}:{}'.format(ip, port))
        self.exposed_id = self.id 

    def __call__(self):
        h = uhash('{}:{}'.format(self.ip, self.port))
        return Pyro4.Proxy('PYRO:{}@{}:{}'.format(h, self.ip, self.port))
