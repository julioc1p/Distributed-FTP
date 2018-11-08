from file_system import FileSystem
from file_system_os import FileSystemOS 
import socket
import os
import time

HOST = '127.0.0.1'
PORT = 21
CWD = '/'


def log(func, cmd):
        logmsg = time.strftime("%Y-%m-%d %H-%M-%S [-] " + func)
        print("\033[31m%s\033[0m: \033[32m%s\033[0m" % (logmsg, cmd))


class FTPServer:


    def __init__(self, cmd_sock, address, file_system):
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
        self.dataSock.send(data.encode())


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
        self.sendCommand('257 "%s".\r\n' % self.cwd)

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
        self.serverSock.bind((HOST, 0))
        self.serverSock.listen(5)
        addr, port = self.serverSock.getsockname( )
        self.sendCommand('227 Entering Passive Mode (%s,%u,%u).\r\n' %
            (','.join(addr.split('.')), port>>8&0xFF, port&0xFF))

    def PORT(self,cmd):
        log("PORT: ", cmd)
        if self.pasv_mode:
            self.serverSock.close()
            self.pasv_mode = False
        l=cmd[5:].split(',')
        self.dataSockAddr='.'.join(l[:4])
        self.dataSockPort=(int(l[4])<<8)+int(l[5])
        self.sendCommand('200 Get port.\r\n')

    def LIST(self, dirpath):
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')
            return

        if not dirpath:
            pathname = self.cwd
        elif dirpath.startswith('/'):
            pathname = dirpath
        else:
            pathname = os.path.join(self.cwd, dirpath)

        log('LIST', pathname)
        if not self.authenticated:
            self.sendCommand('530 User not logged in.\r\n')

        elif not self.file_system.exists(pathname):
            self.sendCommand('550 LIST failed Path name not exists.\r\n')

        else:
            self.sendCommand('150 Here is listing.\r\n')
            self.startDataSock()
            if not self.file_system.isdir(pathname):
                fileMessage = self.file_system.fileProperty(pathname)
                self.dataSock.sock(fileMessage+'\r\n')

            else:
                for file in self.file_system.list(pathname):
                    fileMessage = self.file_system.fileProperty(os.path.join(pathname, file))
                    self.sendData(fileMessage+'\r\n')
            self.stopDataSock( )
            self.sendCommand('226 List done.\r\n')




def serverListener( ):
    global listen_sock
    listen_sock = socket.socket()
    listen_sock.bind((HOST, PORT))
    listen_sock.listen(5)

    log('Server started', 'Listen on: %s, %s' % listen_sock.getsockname( ))
    while True:
        connection, address = listen_sock.accept( )
        log('Accept', 'Created a new connection %s, %s' % address)
        f = FTPServer(connection, address, FileSystemOS() )
        f.run()


serverListener()