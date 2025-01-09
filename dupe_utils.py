import os
import shutil
from datetime import datetime

class FileUtil:
    @staticmethod
    def fullpath(filename):
        return os.path.abspath(filename)

    @staticmethod
    def join(path, filename):
        return os.path.abspath(os.path.join(path, filename))

    @staticmethod
    def parent(path):
        return os.path.dirname(path)

    @staticmethod
    def splitpath(path):
        return path.split(os.sep)

    @staticmethod
    def joinpath(parts):
        return os.sep.join(parts)

    @staticmethod
    def create_dir(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def exists(path):
        return os.path.exists(path)

    @staticmethod
    def delete(path):
       try:
           os.remove(path)
       except IsADirectoryError:
           # maybe this is a directory
           shutil.rmtree(path)
       except FileNotFoundError:
           raise Exception(f"FileUtil.delete(): file does not exist: {path}")

    @staticmethod
    def walk(path):
        return os.walk(path)

    @staticmethod
    def size(path):
        return os.path.getsize(path)

    @staticmethod
    def human_readable(size):
        # Define the units
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

        # Initialize the index for units
        unit_index = 0

        # Loop to find the appropriate unit
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        # Return the formatted string
        return f"{size:.2f} {units[unit_index]}"

class ProcessTimer:
    def __init__(self, start=False):
        self.start = None
        if start:
            self.start = datetime.now()
        self.end = None

    def start(self):
        self.start = datetime.now()

    def stop(self):
        if self.start == None:
            raise Exception('ProcessTimer.stop(): timer not started')
        self.end = datetime.now()

    def elapsed(self):
        if not self.end:
            self.end = datetime.now()

        return self.end - self.start

    def elapsed_readable(self):
        td = self.elapsed()
        ret = ' '
        d = td.days
        h = int(td.seconds / 3600)
        m = int((td.seconds / 60) % 60)
        s = int(td.seconds % 60)
        if d != 0:
            ret += f'{d}d '
        if h != 0:
            ret += f'{h}h '
        if m != 0:
            ret += f'{m}m '
        if s != 0:
            ret += f'{s}s '
        ret = ret.strip()
        if ret == '':
            ret = '<1s'
        return ret.strip()
