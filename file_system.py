class FileSystem():

    """
    Lista los archivos que hay en el path
    """
    def list(self, path):
        pass

    """
    Envia el archivo indicado en el path
    """
    def send(self, path):
        pass

    """
    Comienza a recivir un archivo por el socket
    """
    def recv(self, socket):
        pass

    """
    Elimina el archivo indicado por el path
    """
    def delete(self, path):
        pass

    """
    Crea un nuevo archivo en el path indicado
    """
    def create(self, path):
        pass

    """
    Devuelve la cantidad de espacio disponible en el sistema
    """
    def alloc(self):
        pass

    """
    Renombra el archivo designado por el path con el nuevo nombre
    """
    def rename(self, path, new_name):
        pass
