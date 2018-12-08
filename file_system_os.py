from file_system_ import FileSystem
from utils import fileProperty
import os

class FileSystemOS(FileSystem):

    # def __init__(self):
    #     FileSystem.__init__(self)

#ok
    def list(self, pathname):
        if self.isdir(pathname):
            return os.listdir(pathname)
        return []
#ok
    def exists(self, pathname):
        return os.path.exists(pathname)
#ok
    def fileProperty(self, filepath):
        return fileProperty(filepath)
#ok
    def isdir(self, pathname):
        return os.path.isdir(pathname)
#ok
    def mkdir(self, pathname):
        return os.mkdir(pathname)
#ok
    def rmdir(self, pathname):
        import shutil
        shutil.rmtree(pathname)
#ok
    def remove(self, pathname):
        os.remove(pathname)