from file_system_ import FileSystem
from file_system_os import FileSystemOS
from threading import Thread 
from file_system.coordinator import Coordinator
import socket
import os, sys
import time

HOST = '0.0.0.0'
PORT = 21#23230
# CWD = os.getenv('HOME')
CWD = os.path.sep


def log(func, cmd):
        logmsg = time.strftime("%Y-%m-%d %H-%M-%S [-] " + func)
        print("\033[31m%s\033[0m: \033[32m%s\033[0m" % (logmsg, cmd))


class FTPServer(Thread):


    def __init__(self, cmd_sock, address, file_system):
        Thread.__init__(self)
        self.authenticated = False
        self.pasv_mode     = False
        self.rest          = False
        self.cwd           = CWD
        self.cmd_sock      = cmd_sock   # communication socket as command channel
        self.address       = address
        self.file_system = file_system

    def run(self):

        self.sendCommand('220 Welcome to server.\r\n')
        while True:
            try:
                data = self.cmd_sock.recv(1024).rstrip()
                try:
                    cmd = data.decode()
                except AttributeError:
                    cmd = data
                log('Received data', cmd)            
                if not cmd:
                    break
            except socket.error as err:
                log('Received', err)

            try:
                cmd, arg = cmd[:4].strip().upper(), cmd[4:].strip() or None
                func = getattr(self, cmd)
                func(arg)
            except AttributeError as err:
                self.sendCommand('500 Syntax error, command unrecognized.\r\n')
                log('Received', err)

    def sendCommand(self, cmd):
        self.cmd_sock.send(cmd.encode())

    def sendData(self, data):
        self.dataSock.send(data)


    def startDataSock(self):
        log('startDataSock', 'Opening a data channel')
        try:
            self.dataSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.pasv_mode:
                self.dataSock, self.address = self.serverSock.accept( )

            else:
                self.dataSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.dataSock.connect((self.dataSockAddr, self.dataSockPort))
        except socket.error as err:
            log('startDataSock', err)

    def stopDataSock(self):
        log('stopDataSock', 'Closing a data channel')
        try:
            self.dataSock.close( )
            if self.pasv_mode:
                self.serverSock.close( )
        except socket.error as err:
            log('stopDataSock', err)


    ##---------------------##
    ##    FTP Commands     ##
    ##---------------------##
    def USER(self, user):
        log("USER", user)
        if not user:
            self.sendCommand('501 Syntax error in parameters or arguments.\r\n')

        else:
            self.sendCommand('331 User name okay, need password.\r\n')
            self.username = user

    def PASS(self, passwd):
        log("PASS", passwd)
        if not passwd:
            self.sendCommand('501 Syntax error in parameters or arguments.\r\n')

        elif not self.username:
            self.sendCommand('503 Bad sequence of commands.\r\n')

        else:
            self.sendCommand('230 User logged in, proceed.\r\n')
            self.passwd = passwd
            self.authenticated = True

    def PWD(self, cmd):
        log('PWD', cmd)
        self.sendCommand('257 "{}".\r\n'.format(self.cwd))

    def TYPE(self, type):
        log('TYPE', type)
        self.mode = type
        if self.mode == 'I':
            self.sendCommand('200 Binary mode.\r\n')
        elif self.mode == 'A':
            self.sendCommand('200 Ascii mode.\r\n')

    def PASV(self, cmd):
        log("PASV", cmd)
        self.pasv_mode  = True
        self.serverSock = socket.socket()
        # self.serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSock.bind((self.cmd_sock.getsockname()[0], 0))
        self.serverSock.listen(5)
        addr, port = self.serverSock.getsockname( )
        self.sendCommand('227 Entering Passive Mode (%s,%u,%u).\r\n' %
            (','.join(addr.split('.')), port>>8&0xFF, port&0xFF))

    def PORT(self,cmd):
        log("PORT: ", cmd)
        if self.pasv_mode:
            self.serverSock.close()
            self.pasv_mode = False
        l=cmd.split(',')
        self.dataSockAddr='.'.join(l[:4])
        self.dataSockPort=(int(l[4])<<8)+int(l[5])
        self.sendCommand('200 Get port.\r\n')

    def LIST(self, dirpath):
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')
            return

        if not dirpath:
            pathname = self.cwd
        else:
            pathname = os.path.abspath(os.path.join(self.cwd, dirpath))

        log('LIST', pathname)

        if not self.file_system.isdir(pathname):
            self.sendCommand('550 LIST failed Path name not exist.\r\n')

        else:
            self.sendCommand('150 Here is listing.\r\n')
            self.startDataSock()
            if not self.file_system.isdir(pathname):
                fileMessage = self.file_system.fileProperty(pathname)
                self.sendData(fileMessage+b'\r\n')

            else:
                fileMessage = self.file_system.list(pathname)
                # for file in self.file_system.list(pathname):
                #     fileMessage += self.file_system.fileProperty(os.path.join(pathname, file)) + '\n'
                self.sendData(fileMessage +b'\r\n')
            self.stopDataSock( )
            self.sendCommand('226 List done.\r\n')

    def NLST(self, dirpath):
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')
            return

        if not dirpath:
            pathname = self.cwd
        else:
            pathname = os.path.abspath(os.path.join(self.cwd, dirpath))

        log('LIST', pathname)

        if not self.file_system.isdir(pathname):
            self.sendCommand('550 LIST failed Path name not exist.\r\n')

        else:
            self.sendCommand('150 Here is listing.\r\n')
            self.startDataSock()
            if not self.file_system.isdir(pathname):
                fileMessage = self.file_system.fileProperty(pathname)
                self.sendData(fileMessage+b'\r\n')

            else:
                fileMessage = self.file_system.nlist(pathname)
                # for file in self.file_system.list(pathname):
                #     fileMessage += self.file_system.fileProperty(os.path.join(pathname, file)) + '\n'
                self.sendData(fileMessage +b'\r\n')
            self.stopDataSock( )
            self.sendCommand('226 List done.\r\n')

    def CWD(self, dirpath):
        if not dirpath:
            self.sendCommand('250 CWD Command successful.\r\n')
            return

        pathname = os.path.abspath(os.path.join(self.cwd, dirpath))
        log('CWD', pathname)
        if not self.file_system.isdir(pathname):
            self.sendCommand('550 CWD failed Directory not exist.\r\n')
            return
        self.cwd = pathname
        self.sendCommand('250 CWD Command successful.\r\n')

    def CDUP(self, cmd):
        pathname = os.path.abspath(os.path.join(self.cwd, '..'))
        if not self.file_system.isdir(pathname):
            self.sendCommand('550 CDUP failed Directory not exist.\r\n')
            return
        self.cwd = pathname
        log('CDUP', self.cwd)
        self.sendCommand('200 Ok.\r\n')

    def MKD(self, dirname):
        if not dirname:
            self.sendCommand('550 MKD failed Directory, please introduce a dirname.\r\n')
            return

        pathname = os.path.abspath(os.path.join(self.cwd, dirname))
        log('MKD', pathname)
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')
            return
        if self.file_system.isdir(pathname):
            self.sendCommand('550 MKD failed Directory.\r\n')
            return
        #adaptar al nuevo file_system
        else:
            try:
                self.file_system.mkdir(pathname)
                self.sendCommand('257 "{}" Directory created.\r\n'.format(pathname))
            except OSError:
                self.sendCommand('550 MKD failed Directory "{}" already exist.\r\n'.format(pathname))

    def RMD(self, dirname):
        if not dirname:
            self.sendCommand('550 RMD failed Directory not exist.\r\n')            
            return

        pathname = os.path.abspath(os.path.join(self.cwd, dirname))
        log('RMD', pathname)
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')

        if not self.file_system.isdir(pathname):
            self.sendCommand('550 RMD failed Directory "{}" not exist.\r\n'.format(pathname))
        elif not self.file_system.isdir(os.path.dirname(pathname)):
            self.sendCommand('550 RMD failed Directory "{}" not exist.\r\n'.format(os.path.dirname(pathname)))
        else:
            self.file_system.rmdir(pathname)
            self.sendCommand('250 Directory deleted.\r\n')

    def DELE(self, filename):
        if not filename:
            self.sendCommand('550 DELE failed FILE not exist.\r\n')            
            return

        pathname = os.path.abspath(os.path.join(self.cwd, filename))
        log('DELE', pathname)
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')

        elif not self.file_system.exist(pathname):
            self.sendCommand('550 DELE failed File {} not exist.\r\n'.format(pathname))

        else:
            if not self.file_system.remove(pathname):
                self.sendCommand('450 RETR failed File {} bloqued.\r\n'.format(pathname))
                return
            self.sendCommand('250 File deleted.\r\n')

    def RETR(self, filename):
        pathname = os.path.abspath(os.path.join(self.cwd, filename))
        log('RETR', pathname)
        if not self.file_system.exist(pathname):
            self.sendCommand('550 RETR failed File {} not exist.\r\n'.format(pathname))
            return
        mode = 'r'
        if self.mode == 'I':
            mode += '+b'
        if not self.file_system.open(pathname, mode):
            self.sendCommand('450 RETR failed File {} bloqued.\r\n'.format(pathname))
            return
        self.sendCommand('150 Opening data connection.\r\n')
        self.startDataSock( )
        for data in self.file_system.read():
            if not data: break
            self.sendData(data)
        self.stopDataSock( )
        self.sendCommand('226 Transfer complete.\r\n')

    def STOR(self, filename):
        # print(filename)
        if not self.authenticated:
            self.sendCommand('530 STOR failed User not logged in.\r\n')
            return

        pathname = os.path.abspath(os.path.join(self.cwd, filename))
        log('STOR', pathname)
        
        # self.file_system.remove(pathname)\
        mode = 'w'
        if self.mode == 'I':
            mode+='+b'
        if not self.file_system.open(pathname, mode):
            self.sendCommand('450 STOR failed File {} bloqued.\r\n'.format(pathname))
            return
        self.sendCommand('150 Opening data connection.\r\n' )
        self.startDataSock( )
        # print('file openado')
        while True:
            data = self.dataSock.recv(1024)
            # print('recive {}'.format(data))
            if not data: break
            self.file_system.write(data)
            # print('guarde')
        self.file_system.write(None)
        self.stopDataSock( )
        self.sendCommand('226 Transfer completed.\r\n')

    

def start_ftp():
    global listen_sock
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((HOST, PORT))
    listen_sock.listen(5)

def serverListener( ):
    global listen_sock
    start_ftp()
    log('Server started', 'Listen on: %s, %s' % listen_sock.getsockname( ))
    while True:
        connection, address = listen_sock.accept( )
        log('Accept', 'Created a new connection %s, %s' % address)
        f = FTPServer(connection, address, Coordinator() )
        f.start()


if __name__ == "__main__":
    log('Start ftp server', 'Enter q or Q to stop ftpServer...')
    listener = Thread(target=serverListener)
    listener.start( )

    if sys.version_info[0] < 3:
        input = raw_input

    message = input().lower()
    if message == "q":
        listen_sock.close( )
        log('Server stop', 'Server closed')
        sys.exit()
