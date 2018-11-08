class FileSystem():
    """
    Clase que define la estructura de un sistema de ficheros
    """

    def list(self, path):
        """
        Lista los archivos que hay en el path
        """
        pass

    def send(self, path):
        """
        Envia el archivo indicado en el path
        """
        pass

    def recv(self, socket):
        """
        Comienza a recivir un archivo por el socket
        """
        pass

    def delete(self, path):
        """
        Elimina el archivo indicado por el path
        """
        pass

    def create(self, path):
        """
        Crea un nuevo archivo en el path indicado
        """
        pass

    def alloc(self):
        """
        Devuelve la cantidad de espacio disponible en el sistema
        """
        pass

    def rename(self, path, new_name):
        """
        Renombra el archivo designado por el path con el nuevo nombre
        """
        pass
