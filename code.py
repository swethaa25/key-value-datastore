import os
import sys
import time
import threading
import json
from filelock import Timeout,FileLock

class code:
    def __init__(self, file_path=os.getcwd()):
        if not os.path.exists(file_path):
            raise Exception("File path does not exist!")
        self.lock = threading.Lock()
        self.file_path = file_path + '/data.json'
        #file lock acquired.
        self.lock_path=self.file_path+'.lock'
        self.file_lock=FileLock(self.lock_path,timeout=1)
        self.file_lock.acquire()
        try:
            file = open(self.file_path, 'r')
            file_data = json.load(file)
            self.data = file_data
            file.close()
            self.lock.acquire()
            # checks if size of data store less than 1GB.
            if not os.path.getsize(self.file_path) <= 1e+9:
                self.lock.release()
                raise Exception('Data Store size exceeds 1GB!!')
            self.lock.release()
            print('Data Store opened!!')
        except BaseException:
            file = open(self.file_path, 'w')
            self.data = {}
            file.close()
            print('Data Store created!!')

    # creates a key-value pair.
    def create(self, key, value='', ttl=0):
        if isinstance(key, str) and len(key) <= 32:
            if value == '':
                value = '{}'
            if not self.is_json(value):
                raise Exception('Value not a json object!')
            if sys.getsizeof(value) > 16384:
                raise Exception("Size of the value exceeds 16KB!!")
            if not os.path.getsize(self.file_path) <= 1e+9:
                raise Exception('Data Store size exceeds 1GB!!')
            self.lock.acquire()
            if key in self.data.keys():
                self.lock.release()
                raise Exception('Key is already present!!')
            # ttl = time-to-live.
            if ttl != 0:
                ttl = int(time.time()) + abs(int(ttl))
            new_value = {'value': value, 'ttl': ttl}
            self.data[key] = new_value
            json.dump(self.data, fp=open(self.file_path, 'w'), indent=4)
            self.lock.release()
            print('Value added!!')
        elif not isinstance(key, str):
            raise Exception('Key value is not a string!!')
        elif len(key) > 32:
            raise Exception('Key size is capped at greater than 32!!')

    # Reading an existing key in datastore.
    def read(self, key):
        self.lock.acquire()
        if key not in self.data.keys():
            self.lock.release()
            raise Exception('No such key found!')
        ttl = self.data[key]['ttl']
        if (time.time() < ttl) or (ttl == 0):
            self.lock.release()
            return json.dumps(self.data[key]['value'])
        else:
            self.lock.release()
            raise Exception("Unable to read key.Time-To-Live has expired!!")

    # Deleting an existing key in datastore.
    def delete(self, key):
        self.lock.acquire()
        if key not in self.data.keys():
            self.lock.release()
            raise Exception('No such key exists in data store!!')
        ttl = self.data[key]['ttl']
        if time.time() < ttl or (ttl == 0):
            self.data.pop(key)
            json.dump(self.data, fp=open(self.file_path, 'w'), indent=4)
            self.lock.release()
            print("Key-value pair deleted!!")
            return
        else:
            self.lock.release()
            raise Exception("Unable to read key.Time-To-Live has expired!!")

    # checks if value is a JSON object.
    def is_json(self, val):
        try:
            json_object = json.loads(val)
            return True
        except ValueError as e:
            return False
    def __del__(self):
        #file lock released.
        self.file_lock.release()
