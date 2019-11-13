import abc
import re
import os
import json
import time


class Scheduler(object):

    def __init__(self, interval=600):
        self._interval = interval

    def run(self, func, *args, **kwargs):
        while True:
            self._run(func, *args, **kwargs)
            time.sleep(self._interval)

    def _run(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            pass


class Archiver(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, source, target, excludes, expire_seconds):
        self._name = name
        self._source = source
        self._target = target
        self._excludes = [re.compile(exclude) for exclude in excludes]
        self._expire_seconds = expire_seconds

    def get_name(self):
        return self._name

    def get_source(self):
        return self._source

    def get_target(self):
        return self._target

    def get_excludes(self):
        return self._excludes

    def get_expire_seconds(self):
        return self._expire_seconds

    def get_source_file_list(self):
        if not os.path.exists(self._source):
            return

        return os.listdir(self._source)

    def is_file_exclude(self, filename):
        for exclude in self._excludes:
            if exclude.match(filename):
                return True

        return False

    def is_file_expire(self, filename):
        path = os.path.join(self._source, filename)
        stat = os.stat(path)
        now = time.time()

        return (now - stat['st_atime']) > self._expire_seconds

    def get_source_file_list_without_excludes(self):
        file_list = self.get_source_file_list()

        return filter(self.is_file_exclude, file_list)

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass
        
class Configurator(object):
    def __init__(self, cfg_path):
        self._cfg_path = cfg_path
        self._configure = None

    def _load(self):
        # _configure = {
        #     "interval_seconds": 600,
        #     "archives": [
        #         {
        #             "name": "",
        #             "expire_seconds": 60000,
        #             "source": "",
        #             "exclude_files": [
        #             ],
        #             "type": "",
        #             "target": ""
        #         }
        #     ]
        # }
        with open(self._cfg_path) as fp:
            self._configure = json.load(fp)

    def _get(self):
        if self._configure is None:
            self._load()

        return self._configure

    def get_scheduler_interval(self):
        configure = self._get()

        return configure['interval_seconds']

    def list_archives(self):
        configure = self._get()
        return configure['archives']
