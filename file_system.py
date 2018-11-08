class FileSystem():
    """
    Clase que define la estructura de un sistema de ficheros
    """

    def list(self, pathname):
        """
        Lista los archivos que hay en el path
        """
        pass

    def send(self, pathname):
        """
        Envia el archivo indicado en el path
        """
        pass

    def recv(self, socket):
        """
        Comienza a recivir un archivo por el socket
        """
        pass

    def delete(self, pathname):
        """
        Elimina el archivo indicado por el path
        """
        pass

    def create(self, pathname):
        """
        Crea un nuevo archivo en el path indicado
        """
        pass

    def alloc(self):
        """
        Devuelve la cantidad de espacio disponible en el sistema
        """
        pass

    def rename(self, pathname, new_name):
        """
        Renombra el archivo designado por el path con el nuevo nombre
        """
        pass

    def exists(self, pathname):
        """
        Comprueba si existe el path en el sistema
        """
        pass

    def fileProperty(self, filepath):
        """
        Te devuelve las propiedades del fichero especificado por el path
        """
        pass

    def isdir(self, pathname):
        """
        Devuelve True si lo indicado por el path es un directoria del file system
        o FALSE en caso que no
        """
        pass
