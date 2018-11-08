from file_system import FileSystem
from utils import fileProperty
import os

class FileSystemOS(FileSystem):

    def __init__(self):
        FileSystem.__init__(self)


    def list(self, pathname):
        if self.isdir(pathname):
            return os.listdir(pathname)
        return []

    def exists(self, pathname):
        return os.path.exists(pathname)

    def fileProperty(self, filepath):
        return fileProperty(filepath)

    def isdir(self, pathname):
        return os.path.isdir(pathname)