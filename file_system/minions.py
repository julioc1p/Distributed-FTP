import rpyc
import os
import sys
import logging
from threading import Thread
from rpyc.utils.server import ThreadedServer

Data_Dir = ""

log = logging.getLogger('minion')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(name)s --> %(levelname)s - %(message)s')

# Console Logger
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# File Logger
fd = open("file_system.log", 'a')
fh = logging.StreamHandler(fd)
fh.setLevel(logging.DEBUG)


ch.setFormatter(formatter)
fh.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)


class MinionsService(rpyc.Service):

    class exposed_Minion:

        def exposed_write(self, block_id, data, mirror):
            log.debug(f'writing block {block_id}')
            with open(Data_Dir + str(block_id), 'w') as f:
                f.write(data)
                log.info(f'write {data} on {block_id} was ok')
            if len(mirror) > 0:
                log.debug(f'mirroring {block_id} to {mirror}')
                t = Thread(target=self.mirroring,args=(block_id, data, mirror))
                t.start()

        def exposed_read(self, block_id):
            log.debug(f'reading block {block_id}')
            block_addr = Data_Dir + str(block_id)
            if not os.path.isfile(block_addr):
                log.error(f'block {block_id} not valid')
                return None
            with open(block_addr, 'r') as f:
                log.info(f'read of {block_id} successful')
                return f.read()

        def exposed_remove(self, block_id, mirror):
            os.remove(Data_Dir + str(block_id))
            t = Thread(target=self.delete_mirroring, args=(block_id, mirror))
            t.start()

        def delete_mirroring(self, block_id, mirror):
            host, port = mirror[0]
            conn = rpyc.connect(host, port)
            conn.root.remove(block_id, mirror[1:])

        def mirroring(self, block_id, data, mirror):
            host, port = mirror[0]
            conn = rpyc.connect(host, port=port)
            conn.root.write(block_id, data, mirror[1:])


if __name__ == "__main__":
    if len(sys.argv) == 1:
        log.error("You must pass the directory address for files and the port")
    elif len(sys.argv) > 3:
        log.error("Too many arguments")
    else:
        Data_Dir = sys.argv[1]
        if not os.path.isdir(Data_Dir):
            os.mkdir(Data_Dir)
        port = int(sys.argv[2])
        t = ThreadedServer(MinionsService(), port=port)
        log.info(f'minion launched on localhost at {port}')
        t.start()
