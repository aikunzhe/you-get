#!/usr/bin/python3
# vim:fileencoding=utf-8:sw=4:et

import os
import sys
import enum
import time
import json
import heapq
import sqlite3
import threading
import collections

from . import common
from . import thread_monkey_patch
from .util import log

NATIVE=sys.getfilesystemencoding()

def setup_data_folder(appname):
    """Setup data folder for cross-platform"""
    locations = {
            "win32": "%APPDATA%",
            "darwin": "$HOME/Library/Application Support",
            "linux": "$HOME/.local/share",
            }
    if sys.platform in locations:
        data_folder = locations[sys.platform]
    else:
        data_folder = locations["linux"]
    data_folder = os.path.join(data_folder, appname)

    data_folder = os.path.expandvars(data_folder)
    data_folder = os.path.normpath(data_folder)
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    return data_folder

class TaskError(Exception):
    pass

class TaskStatus(enum.Enum):
    Create = 1
    Queue  = 2
    Start  = 3
    Stop   = 4
    Done   = 5

# Sqlite3 DataType converter
def sql_convert_options(abytes):
    """deocode json encoded option dict"""
    opt_dict = json.loads(abytes.decode("latin1"))
    return opt_dict

def sql_convert_playlist(abytes):
    """decode json encode playlist back to Set"""
    playlist = json.loads(abytes.decode("latin1"))
    if playlist is not None:
        playlist= set(playlist)
    return playlist

def sql_convert_enum(abytes):
    astr = abytes.decode("latin1")
    enum_type, _, enum_name = astr.partition(".")
    klass = globals()[enum_type]
    if not issubclass(klass, enum.Enum):
        raise TypeError("Wrong type: {}".format(enum_type))
    return klass[enum_name]

def sql_adapt_enum(enum_var):
    return str(enum_var)

sqlite3.register_converter("JOPTIONS", sql_convert_options)
sqlite3.register_converter("JPLAYLIST", sql_convert_playlist)
sqlite3.register_converter("ENUM", sql_convert_enum)
sqlite3.register_adapter(TaskStatus, sql_adapt_enum)

class YouGetDB:
    """Sqlite database class for program data"""
    def __init__(self, db_fname=None, dirname=None):
        if db_fname is None:
            db_fname = "you-get.sqlite"
        if dirname is None:
            dirname = setup_data_folder("you-get")
        self.dirname = dirname
        self.db_fname = db_fname
        self.path = os.path.join(dirname, db_fname)
        self.db_version = "1.0" # db version
        self.task_tab = "youget_task"
        self.config_tab = "config"
        self.con = None
        self.setup_database()

    def get_version(self):
        """Get db version in the db file"""
        version = None
        try:
            c = self.load_config()
            version = c.get("db_version")
        except sqlite3.OperationalError:
            pass
        return version

    def setup_database(self):
        dirname = os.path.dirname(self.path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        con = self.con = sqlite3.connect(self.path,
                detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        file_version = self.get_version()

        if not os.path.exists(self.path):
            con.execute("PRAGMA page_size = 4096;")
        con.execute('''CREATE TABLE if not exists {} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT UNIQUE,
            options JOPTIONS,          -- download options in json
            priority INTEGER,
            playlist JPLAYLIST,
            title TEXT,
            filepath TEXT,
            status ENUM,
            success INTEGER,
            total_size INTEGER,
            received INTEGER
            )'''.format(self.task_tab))

        #con.execute("DROP TABLE config")
        con.execute('''CREATE TABLE if not exists {} (
            key TEXT UNIQUE,
            value ANY
            )'''.format(self.config_tab))

        if file_version != self.db_version:
            self.save_config({"db_version": self.db_version})
        con.commit()
        return con

    def get_pragma(self, pragma):
        """Get database PRAGMA values"""
        cur = self.con.cursor()
        ret = cur.execute("PRAGMA {};".format(pragma)).fetchall()
        return ret

    def get_task_list(self):
        """Return a list of tasks"""
        cur = self.con.cursor()
        cur.execute("SELECT * FROM {}".format(self.task_tab))
        return list(cur.fetchall())

    def get_task_values(self, origin):
        """Return one task """
        cur = self.con.cursor()
        cur.execute("SELECT * FROM {} where origin=?".format(self.task_tab),
                (origin,))
        return cur.fetchone()

    def get_task_values_by_id(self, aid):
        """Return one task """
        cur = self.con.cursor()
        cur.execute("SELECT * FROM {} where id=?".format(self.task_tab),
                (aid,))
        return cur.fetchone()

    def fixup_task_data(self, data_dict):
        """Encoding none standard data type into latin1 json bytes"""
        if "id" in data_dict:
            del data_dict["id"]

        if "options" in data_dict:
            data_dict["options"] = json.dumps(data_dict["options"]).encode(
                    "latin1")

        if "playlist" in data_dict:
            pl = data_dict["playlist"]
            if pl is not None:
                pl = list(pl)
            data_dict["playlist"] = json.dumps(pl).encode("latin1")
        return data_dict

    def set_task_values(self, origin, data_dict):
        cur = self.con.cursor()

        data_dict = self.fixup_task_data(data_dict)

        keys = data_dict.keys()
        set_list = ["{}=:{}".format(x,x) for x in keys]
        set_str = ", ".join(set_list)

        # make sure origin is in data_dict
        data_dict["origin"] = origin

        cur.execute('UPDATE {} SET {} WHERE origin=:origin'.format(
            self.task_tab, set_str), data_dict)
        self.con.commit()

    def delete_task(self, origins):
        if isinstance(origins, str):
            origins = [origins]
        data = [(x,) for x in origins]
        cur = self.con.cursor()
        cur.executemany('DELETE FROM {} WHERE origin=?'.format(
            self.task_tab), data)
        self.con.commit()

    def add_task(self, data_dict):
        # insert sqlite3 with named placeholder
        data_dict = self.fixup_task_data(data_dict)

        keys = data_dict.keys()
        keys_tagged = [":"+x for x in keys]

        cur = self.con.cursor()
        cur.execute(''' INSERT INTO {} ({}) VALUES ({}) '''.format(
                    self.task_tab,
                    ", ".join(keys),
                    ", ".join(keys_tagged)),
                data_dict)
        self.con.commit()
        return cur.lastrowid

    def save_config(self, config):
        """Save the config_tab table"""
        cur = self.con.cursor()
        #data = [(x, str(y)) for x, y in config.items()]
        data = config.items()
        cur.executemany('''INSERT OR REPLACE INTO {}
                (key, value)
                VALUES(?, ?)
                '''.format(self.config_tab), data)
        self.con.commit()

    def load_config(self):
        """load the config_tab table as a dict"""
        cur = self.con.cursor()
        cur.execute('SELECT key, value FROM {}'.format(self.config_tab))

        return {x[0]: x[1] for x in cur.fetchall()}

    def try_vacuum(self):
        """Try to vacuum the database when meet some threshold"""
        cur = self.con.cursor()
        page_count = self.get_pragma("page_count")[0][0]
        freelist_count = self.get_pragma("freelist_count")[0][0]
        page_size = self.get_pragma("page_size")[0][0]

        #print(page_count, freelist_count, page_count - freelist_count)
        # 25% freepage and 1MB wasted space
        if (float(freelist_count)/page_count > .25
                and freelist_count * page_size > 1024*1024):
            cur.execute("VACUUM;")
            self.commit()

def log_exc(msg=""):
    """Convenient function to print exception"""
    import traceback
    tb_msg = traceback.format_exc(10)
    err_msg = "{}\n{}".format(tb_msg, msg)
    log.e(err_msg)

def my_download_main(download, download_playlist, urls, playlist, **kwargs):
    ret = 1
    task = kwargs.get("task", None)
    del kwargs["task"]
    try:
        common.download_main(download, download_playlist, urls, playlist,
                **kwargs)
    except:
        ret = -1
        log_exc()
    if task is not None:
        if ret < 0:
            task.success += ret
        else:
            task.success = ret

class Task(thread_monkey_patch.TaskBase):
    """Represent a single threading download task"""
    def __init__(self, **options):
        self.options = {
                "url": None,
                "do_playlist": False,
                "output_dir": os.getcwd(),
                "merge": True,
                "extractor_proxy": None,
                "stream_id": None,
                }
        self.options.update(options)
        self.origin = self.options["url"]

        self.id = -1
        self.priority = 100
        self.progress_bar = None
        self.title = None
        self.real_urls = None # a list of urls
        self.filepath = None
        self.playlist = None
        self.thread = None
        self.total_size = 0
        self.received = 0 # keep a record of progress changes
        self.speed = -1
        self.last_update_time = -1
        self.status = TaskStatus.Create # task status
        self.success = 0

        if self.options["do_playlist"]:
            self.playlist = set()

        self.save_event = threading.Event() # db need save
        self.save_event.clear()
        self.update_lock = threading.Lock()
        self.add_database_lock = threading.Lock()

    def add_to_database(self, database):
        """Add the task to database"""
        self.add_database_lock.acquire()
        aid = database.add_task(self.get_database_data())
        self.add_database_lock.release()
        self.id = aid
        return aid

    def get_total(self):
        """Get total size"""
        ret = self.total_size
        if self.progress_bar is not None:
            ret = self.progress_bar.total_size
        return ret

    def changed(self):
        """check if download progress changed since last update"""
        ret = False
        if self.progress_bar:
            ret = self.received != self.progress_bar.received
        return ret

    def update(self):
        """Update task progress"""
        self.update_lock.acquire()
        if self.progress_bar and self.changed():
            now = time.time()
            received = self.progress_bar.received
            received_last = self.received
            then = self.last_update_time

            # calc speed
            if then > 0:
                if received > received_last:
                    self.speed = float(received - received_last)/(now - then)
                elif self.speed != 0:
                    self.speed = 0

            self.last_update_time = now
            self.received = received
        elif self.progress_bar:
            self.speed = 0
        self.update_lock.release()

        return self.received

    def percent_done(self):
        """Calculate downloaded percent"""
        total = self.get_total()
        if total <= 0:
            return 0
        percent = float(self.received * 100)/total
        return percent

    def update_task_status(self, urls=None, title=None,
            file_path=None, progress_bar=None):
        """Called by the download_urls function to setup download status
        of the given task"""
        if urls is not None:
            self.real_urls = urls

        if self.title is None: # setup title only once
            if title is None and file_path is not None:
                title = os.path.basename(file_path)
            if title is not None:
                self.title = title

        if file_path is not None:
            if self.filepath is None:
                self.filepath = file_path
            if self.options["do_playlist"] and file_path not in self.playlist:
                f = os.path.basename(file_path)
                self.playlist.add(f)

        if progress_bar is not None:
            self.progress_bar = progress_bar
            self.total_size = progress_bar.total_size

        self.update()
        self.save_event.set()

    def save_db(self, db):
        self.update()
        current_data = self.get_database_data()
        old_data = db.get_task_values(self.origin)
        if old_data is not None:
            new_info = {}
            old_keys = set(old_data.keys()) # old_data is not really a dict.
            for k, v in current_data.items():
                if (k not in old_keys) or (old_data[k] != v):
                    new_info[k] = v
            if len(new_info) > 0:
                db.set_task_values(self.origin, new_info)
        self.save_event.clear()

    def get_database_data(self):
        """prepare data for database insertion"""
        keys = [ # Task keys for db
                "id",
                "origin",
                "options",
                "priority",
                "title",
                "filepath",
                "status",
                "success",
                "total_size",
                "received",
                "playlist",
                ]
        data = {x: getattr(self, x, None) for x in keys }
        data["total_size"] = self.get_total()
        return data

    # Override TaskBase() Here
    def pre_thread_start(self, athread):
        athread.name = self.origin

    def target(self, *dummy_args, **dummy_kwargs):
        """Called by the TaskBase start a task thread"""
        self.status = TaskStatus.Start
        self.save_event.set()

        options = self.options
        args = (common.any_download, common.any_download_playlist,
                [self.origin], options["do_playlist"])
        kwargs = {
                "output_dir": options["output_dir"],
                "merge": options["merge"],
                "info_only": False,
                "task": self,
                }
        if options["extractor_proxy"]:
            kwargs["extractor_proxy"] = options["extractor_proxy"]

        if options["stream_id"]:
            kwargs["stream_id"] = options["stream_id"]

        ret = my_download_main(*args, **kwargs)

        self.speed = -1
        self.status = TaskStatus.Stop
        self.save_event.set()
        return ret

class PriorityQueue:
    """See the "8.5.2. Priority Queue Implementation Notes" of heapq doc"""
    REMOVED = "<removed-task>"
    def __init__(self):
        import itertool
        self.queue = []
        self.counter = itertool.count() # to order tasks with same priority
        self.entry_finder = {}

    def push(self, task, priority):
        # reverse priority since the heapq is min head
        priority = -priority
        if task in self.entry_finder:
            self.remove_task(task)
        count = next(self.counter)
        entry = [priority, count, task]
        self.entry_finder[task] = entry
        heapq.heappush(self.queue, entry)

    def remove(self, task):
        """Remove a task from the priority queue"""
        # We actually mark the entry as been removed and del from entry_finder
        entry = self.entry_finder.pop(task)
        entry[-1] = self.REMOVED

    def pop(self):
        """Pop largest priority task"""
        # loop until we found an non-removed task
        while self.queue:
            priority, count, task = heapq.heappop(self.queue)
            if task is not self.REMOVED:
                del self.entry_finder[task]
                return task
        raise KeyError("pop from an empty priority queue")

    def __len__(self):
        return len(self.entry_finder)

    def __contains__(self, task):
        return task in self.entry_finder

    def all(self):
        """Return all tasks as a list"""
        tasks = self.entry_finder.keys()
        return tasks

class TaskManager:
    """Task Manager for multithreading download
    Need to monkey_patch some functions to work. See thread_monkey_patch"""
    def __init__(self, app):
        self.app = app
        self.tasks = collections.OrderedDict()
        self.task_running_queue = []
        self.task_waiting_queue = collections.deque()
        self.max_task = 5
        self.max_retry = 3
        self.thread_local = threading.local()

    def get_database(self):
        """Sqlite connection cannot be shared across thread"""
        if hasattr(self.thread_local, "database"):
            return self.thread_local.database

        database = self.app.database
        if database is None:
            database = self.app.new_database()
        self.thread_local.database = database
        return database

    def start_download(self, info):
        """Start a download task in a new thread"""
        database = self.get_database()

        atask = self.new_task(**info)
        atask.add_to_database(database)
        self.attach_task(atask)
        return atask

    def normalize_task_list(self, tasks):
        """fix tasks into a list of Task objects"""
        if isinstance(tasks, str) or not hasattr(tasks, "__iter__"):
            tasks = [tasks]
        task_list = []
        for atask in tasks:
            if not isinstance(atask, Task):
                atask = self.get_task(atask)
            if atask:
                task_list.append(atask)
        return task_list

    def queue_tasks(self, tasks):
        """Add a task to download queue"""
        task_list = self.normalize_task_list(tasks)
        for atask in task_list:
            if atask.success < 0:
                atask.success = 0

            if atask in self.task_waiting_queue:
                continue

            atask.status = TaskStatus.Queue
            atask.save_event.set()
            self.task_waiting_queue.append(atask)
        self.update_task_queue()

    def stop_tasks(self, tasks):
        """Stop a task by remove it from download queue and running queue"""
        task_list = self.normalize_task_list(tasks)
        for atask in task_list:
            if atask in self.task_waiting_queue:
                self.task_waiting_queue.remove(atask)
                atask.status = TaskStatus.Stop
            if atask in self.task_running_queue:
                self.task_running_queue.remove(atask)
                atask.status = TaskStatus.Stop
            atask.stop()
        self.update_task_queue()

    def start_tasks(self, tasks):
        """Start tasks by remove from download queue and append running queue"""
        task_list = self.normalize_task_list(tasks)
        for atask in task_list:
            if atask in self.task_running_queue:
                continue

            if atask in self.task_waiting_queue:
                self.task_waiting_queue.remove(atask)
            self.task_running_queue.append(atask)
            atask.start()
        self.update_task_queue()

    def get_running_tasks(self):
        return self.task_running_queue

    def update_tasks(self):
        """Update status of tasks and task queues"""
        for atask in self.task_running_queue:
            atask.update()
        self.update_task_queue()

    def update_task_queue(self):
        """Remove finished queue and start new task in waiting queue"""
        database = self.get_database()

        if len(self.task_running_queue) > 0:
            for atask in list(self.task_running_queue):
                if not atask.thread.is_alive():
                    self.task_running_queue.remove(atask)
                    # re-queue on failure
                    if -self.max_retry < atask.success < 0:
                        self.task_waiting_queue.append(atask)
                        atask.status = TaskStatus.Queue
                    atask.save_event.set()
                    atask.thread = None
                    atask.progress_bar = None

        run_queue = self.task_running_queue
        run_tasks = []
        try:
            if len(run_queue) < self.max_task:
                available_slot = self.max_task - len(run_queue)
                for i in range(available_slot):
                    atask = self.task_waiting_queue.popleft()
                    run_tasks.append(atask)
        except IndexError as err:
            #print(err)
            pass
        if len(run_tasks) > 0:
            self.start_tasks(run_tasks)

        # save tasks that need to be saved
        for atask in self.get_tasks():
            if atask.save_event.is_set():
                atask.save_db(database)

    def has_task(self, origin):
        ret = origin in self.tasks
        return ret

    def get_tasks(self):
        """Get all the tasks"""
        ret = self.tasks.values()
        return ret

    def attach_task(self, atask, queue=False):
        """attach a task to task manager"""
        self.tasks[atask.origin] = atask
        if queue == True:
            self.queue_tasks([atask])

    def new_task(self, **task_info):
        """create a new task, not attached to task_manager"""
        err_msg = None
        origin = task_info.get("url", None)

        if origin is None:
            err_msg = "No url found in the task_info"
        elif origin in self.tasks:
            err_msg = "Task for the URL: {} already exists".format(origin)
        if err_msg is not None:
            raise(TaskError(err_msg))

        atask = Task(**task_info)
        return atask

    def get_task(self, origin):
        """Get a task"""
        ret = self.tasks.get(origin, None)
        return ret

    def get_successed_tasks(self):
        ret = []
        for atask in self.get_tasks():
            if atask.success > 0:
                ret.append(atask)
        return ret

    def get_failed_tasks(self):
        ret = []
        for atask in self.get_tasks():
            if atask.success < 0:
                ret.append(atask)
        return ret

    def remove_tasks(self, tasks):
        """Remove tasks from TaskManager and Database"""
        database = self.get_database()

        task_list = self.normalize_task_list(tasks)
        self.stop_tasks(task_list)
        origins = [t.origin for t in task_list]
        for origin in origins:
            del self.tasks[origin]
        database.delete_task(origins)

    def load_tasks_from_database(self):
        """Load saved tasks to TaskManager from database"""
        database = self.get_database()

        tasks = database.get_task_list()
        task_objs = []
        queued = []
        for row in tasks:
            #print(dict(zip(row.keys(), list(row))))#; sys.exit()
            try:
                atask = self.new_task(url=row["origin"])
                self.attach_task(atask)
                for key in row.keys():
                    if hasattr(atask, key):
                        setattr(atask, key, row[key])

                #for k in row.keys(): print(row[k])
                if atask.status in [TaskStatus.Queue, TaskStatus.Start]:
                    queued.append(atask)

                task_objs.append(atask)
            except TaskError as e:
                log.w(str(e))
        if queued:
            self.queue_tasks(queued)
        return task_objs

def main():
    def set_stdio_encoding(enc=NATIVE):
        import codecs; stdio = ["stdin", "stdout", "stderr"]
        for x in stdio:
            obj = getattr(sys, x)
            if not obj.encoding: setattr(sys,  x, codecs.getwriter(enc)(obj))
    set_stdio_encoding()

if __name__ == '__main__':
    main()

