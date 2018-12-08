import logging
import math
import rpyc
import sys
import random
import grp
import pwd
import time
import os
import stat
from file_system.misc import uhash


log = logging.getLogger('coordinator')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(name)s --> %(levelname)s - %(message)s')

# Console Logger
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

# File Logger
fd = open("file_system.log", 'a')
fh = logging.StreamHandler(fd)
fh.setLevel(logging.DEBUG)


fh.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)


def sigint(signal, frame):
    fd.close()
    sys.exit(0)


def ping(ip, port):
    try:
        c = rpyc.connect(ip, port)
        return c.root.alive()
    except :
        return False

def fileProperty(filepath):
    """
    return information from given file, like this "-rw-r--r-- 1 User Group 312 Aug 1 2014 filename"
    """
    st = os.stat(filepath)
    fileMessage = [ ]
    def _getFileMode( ):
        modes = [
            stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR,
            stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP,
            stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH,
        ]
        mode     = st.st_mode
        fullmode = ''
        fullmode += os.path.isdir(filepath) and 'd' or '-'

        for i in range(9):
            fullmode += bool(mode & modes[i]) and 'rwxrwxrwx'[i] or '-'
        return fullmode

    def _getFilesNumber( ):
        return str(st.st_nlink)

    def _getUser( ):
        return pwd.getpwuid(st.st_uid).pw_name

    def _getGroup( ):
        return grp.getgrgid(st.st_gid).gr_name

    def _getSize( ):
        return str(st.st_size)

    def _getLastTime( ):
        return time.strftime('%b %d %H:%M', time.gmtime(st.st_mtime))
    for func in ('_getFileMode()', '_getFilesNumber()', '_getUser()', '_getGroup()', '_getSize()', '_getLastTime()'):
        fileMessage.append(eval(func))
    fileMessage.append(os.path.basename(filepath))
    return ' '.join(fileMessage)


class Coordinator:
    minios = [('localhost', 8000), ('localhost', 8001), ('localhost', 8002)]
    dht_cache = [('localhost', 23235),('localhost', 23237),('localhost', 23239)]
    block_size = 5
    rep_factor = 3

    def __init__(self):
        self.package_count = 1
        if not self.exist(os.path.sep):
            dht = self.get_dht()
            c = rpyc.connect(dht[0],dht[1])
            c.root.set_key(os.path.sep,'')
            c.close()
        try:
            os.mkdir('/tmp/dftp')
        except:
            pass

    def get_minions(self):
        return random.sample(self.minios,1)

    def get_dht(self):
        for i in self.dht_cache:
            if ping(i[0], i[1]):
                return i
        return 'localhost', 23235

    def fileProperty(self,filename):
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(os.path.dirname(filename))[1]
        t = t.split('\n')
        c.close()
        for i in t:
            if os.path.basename(filename) == i.split(' ')[-1]:
                return i

    def nlist(self,file):
        if file == '/':
            file = ''
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(file + os.path.sep)[1]
        te = t.split('\n')
        f = " ".join(te[0].split(' ')[8:])
        for i in te[1:]:
            f += '\n' + " ".join(i.split(' ')[8:])
        return f

    def list(self, file):
        if file == '/':
            file = ''
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(file + os.path.sep)[1]
        c.close()
        return t

    def read_file(self, filename):
        log.debug('getting location for %s', filename)
        dht = self.get_dht()
        file_table = rpyc.connect(dht[0], dht[1]).root.get_key(filename)
        if filename is None:
            return 'No File Found'
        r = file_table[1]
        hosts = {}
        for part in r.split(';'):
            block_location = part.split(':')
            for h in block_location[1].split('@'):
                host = h.split(',')
                host = host[0], int(host[1])
                if not ping(host[0], int(host[1])):
                    continue
                hosts[block_location[0]] = [host]

        hosts = list(hosts.values())

        for i in len(hosts):
            c = rpyc.connect(hosts[i][0], hosts[i][1])
            r += c.root.read(filename + '.part' + str(i))
            yield r
            c.close()
        return None

#Pone '/' al filename
    def isdir(self, filename):
        if filename == os.path.sep:
            return True
        return self.exist(filename + os.path.sep) 

#pone '/' a filename
    def mkdir(self, filename):
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        #Agrega el filename/ al dht
        temp = filename + os.path.sep
        c.root.set_key(temp, '')
        #Path hasta el directorio padre
        dir_name = os.path.dirname(filename)
        #Pone / al final de path del padre
        if not dir_name[-1] == os.path.sep:
            dir_name += os.path.sep
        info = c.root.get_key(dir_name)[1]
        #creamos una carpeta temporal para sacar sus properties
        os.mkdir('tmp_')
        property_ = fileProperty('tmp_')
        property_ = property_[:-4] + os.path.basename(filename)
        os.rmdir('tmp_')
        if info == '':
            info = property_
        else:
            info += '\n' + property_
        c.root.set_key(dir_name,info)
        c.close()
        
    def write_override(self, filename, data):
        o_name = os.path.basename(filename)
        o_dir = os.path.dirname(filename) + os.path.sep
        dist = random.sample(self.get_minions(), min(
            self.rep_factor, len(self.minios)))
        c = rpyc.connect(dist[0][0], dist[0][1])
        if data is None:
            property_ = fileProperty(f'/tmp/dftp/{o_name}')
            k = c.root.get_key(o_dir)[1]
            if k == '':
                k = property_
            else:
                k += '\n' + property_
            c.root.set_key(o_dir, k)
            os.remove(f'/tmp/dftp/{o_name}')
            c.close()
            return
        c.root.write(filename + '.part' +
                     str(self.package_count), data, dist[1:])
        c.close()
        try:
            os.mkdir('/tmp/dftp')
        except:
            pass
        f = open(f'/tmp/dftp/{o_name}', 'a+b')
        f.write(data)
        f.close()
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(filename)[1]
        name = []
        for i in dist:
            name.append(i[0] + str(dist[1]))
        name = '@'.join(name)
        name = str(self.package_count) + name
        if t is None:
            c.root.set_key(filename, name)
        else:
            t += ';' + name
            c.root.set_key(filename, t)
        c.close()
        self.package_count += 1

    def write_append(self, filename, data):
        o_name = os.path.basename(filename)
        o_dir = os.path.dirname(filename) + os.path.sep
        dist = random.sample(self.get_minions(), min(self.rep_factor, len(self.minios)))
        c = rpyc.connect(dist[0][0], dist[0][1])
        if data is None:
            property_ = fileProperty(f'/tmp/dftp/{o_name}')
            k = c.root.get_key(o_dir)[1]
            if k == '':
                k = property_
            else:
                k += '\n' + property_
            c.root.set_key(o_dir, k)
            os.remove(f'/tmp/dftp/{o_name}')
            c.close()
            return
        c.root.write(filename + '.part' + str(self.package_count), data, dist[1:])
        c.close()
        
        f = open(f'/tmp/dftp/{o_name}', 'a+b')
        f.write(data)
        f.close()
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(filename)[1]
        name = []
        for i in dist:
            name.append(i[0] + ',' + str(dist[1]))
        name = '@'.join(name)
        name = str(self.package_count) + name
        t += ';' + name
        c.root.set_key(filename, t)
        c.close()
        self.package_count += 1

#no lleva palito
    def exist(self, key):
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        if c.root.get_key(key):
            return True
        return False

    # def calculate_count(self, size):
    #     r = int(math.ceil(float(size)/self.block_size))
    #     log.debug('file will split in %d block', r)
    #     return r

    def rename(self, oldname, newname):
        dht = self.get_dht()
        c = rpyc.connect(dht[0], dht[1])
        key = c.root.remove_key(oldname)
        c.root.set_key(newname, key)
        c.close()
        d_name = os.path.dirname(newname)
        b_name = os.path.basename(newname)
        c = rpyc.connect(dht[0], dht[1])
        info = c.root.get_key(d_name).split('\n')
        i = 0
        while i < len(info):
            info[i] = info[i].split(' ')
            c_name = ' '.join(info[i][8:])
            if c_name == b_name:
                info[i] = info[i][0, 8] + b_name.split(' ')
            info[i] = ' '.join(info[i])
        info = '\n'.join(info)
        c.root.set_key(d_name, info)
        

    def rmdir(self, path):
        f = self.nlist(path).split('\n')
        if not f == [""]:
            for i in f:
                dir = path + os.path.sep + i
                if self.isdir(dir):
                    self.rmdir(dir)
                else:
                    self.remove(dir)
        dht = self.get_dht()
        print(path + os.path.sep)
        c = rpyc.connect(dht[0], dht[1])
        c.root.remove_key(path + os.path.sep)        
        p = os.path.dirname(path)
        s = self.list(p)
        if p != os.path.sep:
            p+=os.path.sep
        s = s.split('\n')
        b_name = os.path.basename(path)
        a = []
        for i in s:
            t = ' '.join(i.split(' ')[8:])
            if t == b_name:
                continue
            a.append(i)
        a = '\n'.join(a)
        print(a)
        dht = self.get_dht()
        rpyc.connect(dht[0], dht[1]).root.set_key(p, a)

    def remove(self, file):
        dht = self.get_dht()
        file_table = rpyc.connect(dht[0], dht[1]).root.remove_key(file)
        r = file_table[1]
        r = file
        hosts = {}
        for part in r.split(';'):
            block_location = part.split(':')
            for h in block_location[1].split('@'):
                host = h.split(',')
                host = tuple(host)
                if not ping(host[0], int(host[1])):
                    continue
                if block_location[0] not in hosts:
                    print([host])
                    hosts[block_location[0]] = [host]
                else:
                    hosts[block_location[0]].append(host)

        for i in hosts:
            for j in hosts[i]:
                c = rpyc.connect(j[0], j[1])
                c.root.remove(file + '.part' + i)
                c.close()
        return None        
