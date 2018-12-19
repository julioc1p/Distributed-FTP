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
from config import *


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
    
    block_size = 5
    rep_factor = 3

    def __init__(self):
        self.package_count = 1
        if not self.exist(os.path.sep):
            dht = self.get_name()
            c = rpyc.connect(dht[0],dht[1])
            c.root.set_key(os.path.sep,'')
            c.close()
        try:
            os.mkdir('/tmp/dftp')
        except:
            pass

    def get_minions(self):
        for i in minions_cache:
            if ping(i[0], i[1]):
                return i

    def get_lock(self):
        for i in lock_cache:
            if ping(i[0], i[1]):
                return i

    def get_name(self):
        for i in dht_cache:
            if ping(i[0], i[1]):
                return i
                
    def nlist(self,file):
        if file == os.path.sep:
            file = ''
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(file + os.path.sep)[1]
        te = t.split('\n')
        f = " ".join(te[0].split(' ')[8:])
        for i in te[1:]:
            f += '\n' + " ".join(i.split(' ')[8:])
        return f.encode()

    def list(self, file):
        if file == os.path.sep:
            file = ''
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(file + os.path.sep)[1]
        c.close()
        return t.encode()

    def read(self):
        if self.filename is None:
            return None
        lock_dht = self.get_lock()
        connect_lock = rpyc.connect(lock_dht[0], lock_dht[1])
        connect_lock.root.lock(self.filename, 1)
        connect_lock.close()
        name_dht = self.get_name()
        connect_name = rpyc.connect(name_dht[0], name_dht[1])
        list_name = connect_name.root.get_key(self.filename)[1]
        connect_name.close()
        hosts = {}
        for part in list_name.split(';'):
            block_id, block_location = part.split(':')
            host = block_location.split(',')
            host = host[0], int(host[1])
            hosts[block_id] = host
        for i in hosts:
            # lock_dht = self.get_lock()
            # connect_lock = rpyc.connect(lock_dht[0], lock_dht[1])
            # connect_lock.root.lock(self.filename, 1)
            # connect_lock.close()
            host = hosts[i]
            if not ping(host[0], host[1]):
                host = self.get_minions()
            
            connect_file = rpyc.connect(host[0], host[1])
            data = connect_file.root.get_key(f'{self.filename}.part{i}')[1]
            connect_file.close()
            if data:
                # if self.mode[-1] == 'b':
                #     t = data.encode()
                #     yield t
                # else:
                yield data
            else:
                return None
        
        lock_dht = self.get_lock()
        connect_lock = rpyc.connect(lock_dht[0], lock_dht[1])
        connect_lock.root.remove_lock(self.filename, 1)
        connect_lock.close()
        self.filename = None
        return None

#Pone os.path.sep al filename
    def isdir(self, filename):
        if filename == os.path.sep:
            return True
        return self.exist(filename + os.path.sep) 

#pone os.path.sep a filename
    def mkdir(self, filename):
        dht = self.get_name()
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
        
    def write(self, data):
        if self.filename is None:
            return None
        o_name = os.path.basename(self.filename)
        o_dir = os.path.dirname(self.filename) 
        if o_dir == os.path.sep:
            o_dir = ''
        o_dir += os.path.sep
        if data is None:

            property_ = fileProperty(f'/tmp/dftp/{o_name}')
            name_dht = self.get_name()
            c = rpyc.connect(name_dht[0], name_dht[1])
            k = c.root.get_key(o_dir)
            if k == None:
                k = property_
            else:
                k = k[1]
                k += '\n' + property_
            c.root.set_key(o_dir, k)
            os.remove(f'/tmp/dftp/{o_name}')
            c.close()

            lock = self.get_lock()
            c = rpyc.connect(lock[0], lock[1])
            c.root.remove_lock(self.filename, 2)
            c.close()

            self.filename = None
            self.package_count = 1
            return 
        #hechar un vistaso 
        # if self.mode[-1] == 'b':
        #     data = data.decode()

        # lock_dht = self.get_lock()
        # c = rpyc.connect(lock_dht[0], lock_dht[1])
        # c.root.lock(self.filename, 2)
        # c.close()

        # if data is bytes: 
        #     t = data
        # else:
        #     t = data.decode()
        
        file_dht = self.get_minions()
        c = rpyc.connect(file_dht[0], file_dht[1])
        # print('coordinator before')
        c.root.set_key(self.filename + '.part' + str(self.package_count), data)
        # print('coordinator after')
        c.close()

        # lock_dht = self.get_lock()
        # c = rpyc.connect(lock_dht[0], lock_dht[1])
        # c.root.lock(self.filename, 2)
        # c.close()

        # print(self.mode)
        f = open(f'/tmp/dftp/{o_name}', 'a+b')#+self.mode[1:])
        # if self.mode[-1] == 'b':
        #     f.write(data.encode())
        # else:
        f.write(data)
        f.close()

        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        t = c.root.get_key(self.filename)
        c.close()
        r = f'{self.package_count}:{file_dht[0]},{file_dht[1]}'
        if t != None:
            t = t[1]
            r = t + ';' + r
        name_dht = self.get_name()
        c = rpyc.connect(name_dht[0], name_dht[1])
        c.root.set_key(self.filename, r)
        c.close()
        self.package_count += 1

#no lleva palito
    def exist(self, key):
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        if c.root.get_key(key):
            return True
        return False

    # def calculate_count(self, size):
    #     r = int(math.ceil(float(size)/self.block_size))
    #     log.debug('file will split in %d block', r)
    #     return r

    def rename(self, oldname, newname):
        lock_dht = self.get_lock()
        c = rpyc.connect(lock_dht[0], lock_dht[1])
        #frase: 'dejame ese a mi !!!!, jj'
        l = c.root.get_key(oldname)
        c.close()
        if not l is None:
            return False
        lock_dht = self.get_lock()
        c = rpyc.connect(lock_dht[0], lock_dht[1])
        l = c.root.set_key(oldname)
        c.close()
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        keys = c.root.remove_key(oldname)
        c.root.set_key(newname, key)
        c.close()
        hosts = {}
        for part in keys.split(';'):
            block_id, block_location = part.split(':')
            host = block_location.split(',')
            host = host[0], int(host[1])
            hosts[block_id] = host
        self.mode = 'w'
        for i in hosts:
            self.filename = oldname
            data = self.read()
            self.filename = newname
            self.write(data)
        self.filename = oldname
        data = self.read()
        self.filename = newname
        self.write(data)
        self.filename = None
        self.package_count = 1
        d_name = os.path.dirname(newname)
        b_name = os.path.basename(newname)
        c = rpyc.connect(dht[0], dht[1])
        info = c.root.get_key(d_name).split('\n')
        c.close()
        i = 0
        while i < len(info):
            info[i] = info[i].split(' ')
            c_name = ' '.join(info[i][8:])
            if c_name == b_name:
                info[i] = info[i][:8] + b_name.split(' ')
            info[i] = ' '.join(info[i])
        info = '\n'.join(info)
        c = rpyc.connect(dht[0],dht[1])
        c.root.set_key(d_name, info)
        c.close()
        

    def rmdir(self, path):
        f = self.nlist(path)
        f = f.decode()
        f = f.split('\n')
        if not f == [""]:
            for i in f:
                dir = path + os.path.sep + i
                if self.isdir(dir):
                    if not self.rmdir(dir):
                        return False
                else:
                    if not self.remove(dir):
                        return False
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        c.root.remove_key(path + os.path.sep)        
        p = os.path.dirname(path)
        s = self.list(p)
        s = s.decode()
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
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        c.root.set_key(p, a)
        return False

    def remove(self, file):
        if not self.exist(file):
            return False
        dht = self.get_name()
        name_dht = self.get_name()
        c = rpyc.connect(name_dht[0], name_dht[1])
        file_tale = c.root.get_key(file)
        if file_tale is None:
            return False
        hosts = {}
        file_tale = file_tale[1]
        lock_dht = self.get_lock()             
        c = rpyc.connect(lock_dht[0], lock_dht[1])
        l = c.root.lock(file, 3)
        c.close()
        if not l:
            return False
        for part in file_tale.split(';'):
            block_id, block_location = part.split(':')
            host = block_location.split(',')
            host = host[0], int(host[1])
            hosts[block_id] = host

        for i in hosts:
            lock_dht = self.get_lock()
            c = rpyc.connect(lock_dht[0], lock_dht[1])
            l_t = c.root.lock(file, 3)
            c.close()
            j = self.get_minions()
            if ping(hosts[i][0], hosts[i][1]):
                j = hosts[i]
            c = rpyc.connect(j[0], j[1])
            c.root.remove_key(file + '.part' + i)
            c.close()

        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        file_tale = c.root.remove_key(file)
        c.close()
        p = os.path.dirname(file)
        s = self.list(p)
        s = s.decode()
        if p != os.path.sep:
            p += os.path.sep
        s = s.split('\n')
        b_name = os.path.basename(file)
        a = []
        for i in s:
            t = ' '.join(i.split(' ')[8:])
            if t == b_name:
                continue
            a.append(i)
        a = '\n'.join(a)
        dht = self.get_name()
        c = rpyc.connect(dht[0], dht[1])
        c.root.set_key(p, a)

        lock_dht = self.get_lock()
        c = rpyc.connect(lock_dht[0], lock_dht[1])
        l = c.root.remove_lock(file, 3)
        c.close()
        return True        

    def open(self, filename, mode):   
        self.filename = filename
        self.mode = mode
        flag = 1
        if mode[0] == 'w':
            self.remove(filename)
            flag = 2
        lock = self.get_lock()
        connection_lock = rpyc.connect(lock[0], lock[1])
        lock = connection_lock.root.lock(filename, flag)
        connection_lock.close()
        # if self.lock is None and (mode[0] == 'w' or mode[0] == 'a'):
        if not lock:  
            return False        
        o_name = os.path.basename(filename)
        if mode[0] == 'a':
            f = open(f'/tmp/dftp/{o_name}', 'w' + mode[1:])
            while 1:
                data = self.read(filename)
                if data == 0:
                    break
                else:
                    f.write(data)
                    self.package_count += 1
            f.close()
        return True

