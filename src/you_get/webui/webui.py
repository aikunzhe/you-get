#!/usr/bin/python3
# vim:fileencoding=utf-8:sw=4:et

import os
import sys
import time
import json
import copy
import queue
import socket
import threading
import collections

from .. import thread_monkey_patch
from .. import task_manager
from ..task_manager import TaskStatus
from .. import common
from ..util import log

import bottle

APPNAME = "you-get"
__version_id__ = "0.1"
LOG_QUEUE_SIZE = 100
MAX_LOG_LINES = 400
LOG_COLOR_CODES = {
        1:  "bold",
        4:  "underline",
        31: "red",
        32: "green",
        33: "yellow",
        34: "blue",
        }

NATIVE=sys.getfilesystemencoding()

socket_timeout = 60
socket.setdefaulttimeout(socket_timeout)

def num2human(num, kunit=1000):
    """Convert integer to human readable units"""
    if isinstance(num, str) and not num.isdigit():
        return num
    human = ""
    num = float(num)
    if num > 0.01:
        units = ['','K','M','G','T','P','E','Z','Y']
        for x in units:
            if num < kunit:
                human = x
                break
            num /= kunit
    else:
         units = ["m", "μ", "n", "p", "f", "a", "z", "y"]
         for x in units:
             num *= kunit
             if num >= 1.0:
                 human = x
                 break

    return "{:.2f}{}".format(num, human)

# http://taoofmac.com/space/blog/2014/11/16/1940
def sse_pack(d):
    """Pack data in SSE format"""
    buffer = ''
    for k in ['retry','id','event','data']:
        if k in d.keys():
            buffer += '%s: %s\n' % (k, d[k])
    return buffer + '\n'

def route_app(web_app):
    """Route instance methods of an object to bottle router
    Try to find out if a method have an attribute like one of "route", "get",
    or "post"... If we found one, route it according to the attribute content.

    The attribute content can be a path str for route decorator or a dict
    which will be passed to the Bottle.route() method
    """
    routers = ["route", "get", "post", "put", "delete", "options"]
    for kw in dir(web_app):
        if kw.startswith("__"):
            continue
        attr = getattr(web_app, kw)
        if not callable(attr):
            continue
        for r in routers:
            if hasattr(attr, r):
                bottle_router = getattr(bottle, r)
                route_value = getattr(attr, r)
                if isinstance(route_value, str):
                    paths = [route_value]
                    route_value = {}
                elif isinstance(route_value, list):
                    paths = route_value
                    route_value = {}
                else:
                    paths = route_value.pop("path")
                for path in paths:
                    route_value["path"] = path
                    #print(route_value)
                    bottle_router(**route_value)(attr)
                break

class WebApp:
    def __init__(self, app):
        self.app = app
        self.retry_timeout = 5000
        self.timeout_sse_keepalive = 30 # 30 second
        self.timeout_sse_sleep = 2
        self.timeout_sse_max = 60*10 # 10 minutes server-sent event

        dot_dir = os.path.dirname(os.path.abspath(__file__))
        self.static_root = os.path.join(dot_dir, "html")
        self.movie_root = self.app.output_dir


    def route_up(self):
        route_app(self)

    def get_task_data(self, atask):
        data = atask.get_database_data()
        data["status"] = data["status"].name
        data["speed"] = atask.speed

        # we don't return output directory or any local directory info
        data = copy.deepcopy(data)
        del data["options"]["output_dir"]
        if data["filepath"]:
            data["filepath"] = os.path.basename(data["filepath"])

        return data

    def query_task_by_type(self, task_type=None):
        if not task_type:
            tasks = self.app.get_tasks()
            data_ls = {x.id: self.get_task_data(x) for x in tasks}
        elif task_type == "successed":
            data_ls = self.get_tasks_successed()
        elif task_type == "failed":
            data_ls = self.get_tasks_failed()
        else:
            data_ls = {}
        return data_ls

    def _query_updated_task(self, last_update_time):
        """Get task data for tasks updated since last_update_time"""
        task_ls = self.app.get_tasks()
        task_datas = {x.id: self.get_task_data(x) for x in task_ls if
                x.last_update_time > last_update_time}
        if len(task_datas):
            last_update_time = max([x.last_update_time for x in task_ls])
        return task_datas, last_update_time

    def get_tasks(self):
        """return a dict with task id as key, task data as value.
        """
        request = bottle.request

        task_type = bottle.request.query.type
        ifmod = "If-Modified-Since"
        if task_type or ifmod not in request.headers:
            data_ls = self.query_task_by_type(task_type)
        else:
            last_update_time = bottle.parse_date(
                    request.headers.get('If-Modified-Since'))
            data_ls, last_u = self._query_updated_task(last_update_time)
            if len(data_ls) <= 0:
                # response.status = 304 # firefox cannot handle 304 on xhr
                return ""
        return data_ls
    get_tasks.get ="/api/ver1/tasks"

    def get_tasks_server_sent_event(self):
        """A Server-Sent Event handler to do server push"""
        last_update_time = -2
        event_id = 0

        request = bottle.request
        response = bottle.response

        # Keep event IDs consistent
        if 'Last-Event-Id' in request.headers:
            event_id = int(request.headers['Last-Event-Id']) + 1
        response.headers['content-type'] = 'text/event-stream'
        response.headers['cache-control'] = 'no-cache'
        #response.headers['Access-Control-Allow-Origin'] = '*'

        msg = { "retry": self.retry_timeout }
        yield sse_pack(msg);
        event_id += 1

        msg = { 'event': 'tasks' }

        qdata = self._query_updated_task(last_update_time)
        task_datas, last_update_time = qdata

        msg.update({"data": json.dumps(task_datas), "id": event_id})
        yield sse_pack(msg);
        event_id += 1

        heartbeat_count = 0
        total_count = 0
        timeout_sleep = self.timeout_sse_sleep
        timeout_keepalive = self.timeout_sse_keepalive // timeout_sleep
        timeout_max = self.timeout_sse_max // timeout_sleep
        while True:
            send_msg = None

            qdata = self._query_updated_task(last_update_time)
            if len(qdata[0]) <= 0:
                if heartbeat_count >= timeout_keepalive:
                    send_msg = ":\n" # heartbeat
            else:
                task_datas, last_update_time = qdata
                msg.update({ "data": json.dumps(task_datas), "id": event_id})
                send_msg = sse_pack(msg);
                event_id += 1

            if send_msg is not None:
                yield send_msg
                heartbeat_count = 0

            if total_count >= timeout_max:
                break
            time.sleep(timeout_sleep)
            heartbeat_count += 1
            total_count += 1
    get_tasks_server_sent_event.get ="/api/ver1/tasks/sse"

    def get_tasks_successed(self):
        """return a dict with task id as key, task data as value.
        """
        tasks = self.app.get_tasks_successed()
        data_ls = {x.id: self.get_task_data(x) for x in tasks}
        return data_ls
    get_tasks_successed.get = "/api/ver1/tasks/successed"

    def get_tasks_failed(self):
        """return a dict with task id as key, task data as value.
        """
        tasks = self.app.get_tasks_failed()
        data_ls = {x.id: self.get_task_data(x) for x in tasks}
        return data_ls
    get_tasks_failed.get = "/api/ver1/tasks/failed"

    def get_task_by_id(self, task_id):
        """return a dict of task data"""
        atask = self.app.get_task_by_id(id)
        data = self.get_task_data(atask)
        return data
    get_task_by_id.get = "/api/ver1/tasks/<id:int>"

    def new_task(self):
        """return a dict with new task id as key, data as value.
        On error, return a dict with "error" key.
        """
        keys = ["urls", "do_playlist", "merge", "stream_id", "extractor_proxy"]
        options = {}
        #print(dict(bottle.request.forms))
        #print(dict(bottle.request.json));return {}
        form_json = bottle.request.json

        bool_values = ["do_playlist", "merge"]
        for k in keys:
            v = form_json.get(k, None)
            if v:
                options[k] = v

        for k in bool_values:
            if k in options:
                options[k] = True
        #print(options); return {}
        try:
            tasks = self.app.new_task(options)
            if not tasks:
                data = {"error": "Failed to create new download."}
            data = {x.id: self.get_task_data(x) for x in tasks}
        except task_manager.TaskError as err:
            msg = str(err)
            data = {"error": msg}
        return data
    new_task.post = "/api/ver1/tasks"

    def delete_task_by_id(self, id):
        """return emptry dict"""
        self.app.delete_task_by_id(id)
        return {}
    delete_task_by_id.delete = "/api/ver1/tasks/<id:int>"

    def delete_tasks(self):
        """return emptry dict"""
        #print(dict(bottle.request.json))#;return {}
        form_json = bottle.request.json
        ids = form_json.get("ids", None)
        if ids is None or len(ids) <= 0:
            return {"error": "No task ids found"}
        ids = set(form_json["ids"])
        tasks = self.app.get_tasks()
        tasks_del = [x for x in tasks if x.id in ids]
        #print(tasks_del);return {}
        if len(tasks_del) > 0:
            self.app.delete_tasks(tasks_del)
        return {}
    delete_tasks.delete = "/api/ver1/tasks"

    def update_task_status_by_id(self, id):
        """return emptry dict"""
        form_json = bottle.request.json
        if "status" in form_json:
            self.app.update_task_status_by_id(id, form_json["status"])
        return {}
    update_task_status_by_id.put = "/api/ver1/tasks/<id:int>/status"

    def update_tasks_status(self):
        """change status of tasks. return emptry dict"""
        form_json = bottle.request.json
        for tid in form_json.keys():
            if "status" in form_json[tid]:
                self.app.update_task_status_by_id(tid, form_json[tid]["status"])
        return {}
    update_tasks_status.put = "/api/ver1/tasks/status"

    def get_log(self):
        """return log data as a multiline string"""
        data = self.app.get_log()
        return data
    get_log.get = "/api/ver1/log"

    def get_movies(self):
        """return file info for movies:
            {"files": [(filename, size, date), ...]}"""
        file_list = os.listdir(self.movie_root)
        file_list = [x for x in file_list
                if not (x.startswith(".") or x.endswith(".downloading"))]
        datas = []
        for f in file_list:
            try:
                fname = os.path.join(self.movie_root, f)
                fsize = os.path.getsize(fname)
                ftime = os.path.getmtime(fname)
                datas.append([f, fsize, ftime])
            except os.error:
                pass
        result = {"files": datas}
        return result
    get_movies.get = ["/api/ver1/movies"]

    ### static web pages ###
    def html_files(self, path="index.html"):
        return bottle.static_file(path, root=self.static_root)
    html_files.route = ["/html", "/html/", "/html/<path:path>"]

    def movie_download_files(self, path):
        # force download filename to be latin1 encoded unicode, i.e. bytes
        dlname = path.replace('"', "_").encode("utf-8").decode("latin1")
        return bottle.static_file(path, root=self.movie_root, download=dlname)
    movie_download_files.route = "/movies/download/<path>" # one level deep

    def movie_files(self, path):
        return bottle.static_file(path, root=self.movie_root)
    movie_files.route = "/movies/<path>" # one level deep


class MonkeyFriend(thread_monkey_patch.UIFriend):
    """UI object for the thread_monkey_patch.UI_Monkey"""
    def __init__(self, app):
        self.app = app

    def sprint(self, text, *colors):
        """Called by download thread to output something"""
        lqueue = self.app.log_queue
        lqueue.put((text, colors))

class App:
    """main application"""
    db_fname = "you-get-web.sqlite"
    cookie_fname = "you-get-web-cookies.txt"
    defaults = {
            "host": "localhost",
            "port": 8080,
            "data_dir": os.path.join(task_manager.setup_data_folder(APPNAME),
                "webui"),
            "output_dir": os.getcwd(),
            "debug": False,
            }
    def __init__(self, args):
        self.bottle_app = bottle.default_app()
        self.cookiejar = None
        self.config_path = None
        self.load_config(args)
        self.task_manager_lock = threading.Lock()
        config = self.bottle_app.config

        self.task_manager = task_manager.TaskManager(self)
        self.data_dir = config["youget.data_dir"]
        self.output_dir = config["youget.output_dir"]

        self.text_log = None
        self.max_log_lines = MAX_LOG_LINES
        self.log_queue = queue.Queue(LOG_QUEUE_SIZE)

        # queue task_manager actions to running in a single thread
        self.task_action_queue = queue.Queue()
        self.task_manager_running = False

        #self.queue_action(self.delay_init)
        self.database = None
        self.web_app = WebApp(self)
        self.web_app.route_up()

    def new_database(self):
        database = task_manager.YouGetDB(self.db_fname, self.data_dir)
        return database

    def delay_init(self):
        self.setup_web()
        self.setup_cookiejar()
        self.load_tasks_from_database()

    def update_task(self, sleep_timeout=1):
        """Event loop to update task manager events
        Should run in a thread that update task manager
        """

        counter = 0
        while self.task_manager_running == True:
            try:
                self.task_manager_lock.acquire()
                if counter % 3 == 0:
                    self.task_manager.update_tasks()
                    counter = 0
                self.check_log()
                max_task = 32 # max number of actions to do in one iteration

                for tasks in self.task_manager.get_tasks():
                    if tasks.thread is None and tasks.status == TaskStatus.Stop:
                        tasks.status = TaskStatus.Done

                # run queue methods in task_action_queue
                try:
                    for i in range(max_task):
                        func, args, kwargs = self.task_action_queue.get(False)
                        func(*args, **kwargs)
                except queue.Empty:
                    pass
            finally:
                self.task_manager_lock.release()

            counter += 1
            time.sleep(sleep_timeout)

    def setup_web(self):
        self.text_log = collections.deque(maxlen=self.max_log_lines)

    def load_tasks_from_database(self):
        tasks = self.task_manager.load_tasks_from_database()
        return tasks

    def check_log(self):
        """Check log_queue and output log messages"""
        lqueue = self.log_queue
        try:
            for i in range(LOG_QUEUE_SIZE):
                text, colors = lqueue.get(block=False)
                self.text_log.append((text, colors))
        except queue.Empty:
            pass

    def load_config(self, args):
        config = self.bottle_app.config
        for k in self.defaults.keys():
            youget_k = "youget." + k
            config.setdefault(youget_k, self.defaults[k])

        if os.path.exists(args.config):
            self.config_path = args.config
            log.i("Use config file: {}".format(self.config_path))
            config.load_config(self.config_path)
        for k in args.__dict__.keys():
            v = getattr(args, k)
            if v is not None:
                config["youget.{}".format(k)] = v

        if args.output_dir:
            config["youget.output_dir"] = os.path.abspath(args.output_dir)
        if args.data_dir:
            config["youget.data_dir"] = os.path.abspath(args.data_dir)
        if args.debug:
            config["youget.debug"] = args.debug
        log.i("Output dir: {}".format(config["youget.output_dir"]))
        log.i("Data dir: {}".format(config["youget.data_dir"]))

    def save_config(self):
        config = self.bottle_app.config

    def setup_cookiejar(self):
        """setup cookie jar
        It seems cookiejar use some kind Lock, so assume we are thread safe"""
        from http import cookiejar
        cookie_path = os.path.join(self.data_dir, self.cookie_fname)
        cookies_txt = cookiejar.MozillaCookieJar(cookie_path)
        try:
            cookies_txt.load()
        except OSError:
            pass # file not found
        common.cookies_txt = cookies_txt
        self.cookiejar = cookies_txt

    def start(self):
        """start task update and some other things"""
        self.task_thread = threading.Thread(target=self.update_task)
        self.task_manager_running = True
        self.task_thread.start()
        time.sleep(0.1)
        self.delay_init()

    def stop(self):
        self.task_manager_running = False
        self.clean_up()

    def run_server(self, server=None):
        """Actually start the bottle httpd server"""
        options = {} # options passed to run()
        if server is None:
            config = self.bottle_app.config
            server_name = config.get("youget.server_type", None)
            if server_name:
                server = bottle.server_names[server_name]
            else:
                patch_wsgiref_broken_pipe_error()
                from socketserver import ThreadingMixIn
                from wsgiref.simple_server import WSGIServer
                class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
                    daemon_threads = True

                server = bottle.WSGIRefServer
                # see bottle.py for the "server_class" options
                options["server_class"] = ThreadingWSGIServer


        bottle.run(server=server,
                host=config["youget.host"],
                port=config["youget.port"],
                debug=config["youget.debug"], **options)

    def clean_up(self, *args):
        print("cleaning up...")
        self.save_config() # need to done before mainloop stop

        self.cookiejar.save()
        database = self.new_database()
        task_items = self.task_manager.get_running_tasks()
        for atask in task_items:
            atask.save_db(database)

        task_items = self.task_manager.get_tasks()
        for atask in task_items:
            if atask.save_event.is_set():
                atask.save_db(database)
        database.try_vacuum()

    def queue_action(self, func, *args, **kwargs):
        """Queue a function to be called in task_manager thread"""
        self.task_action_queue.put((func, args, kwargs))

    ### actions ###
    def get_tasks(self):
        """get a list of all the tasks"""
        return self.task_manager.get_tasks()

    def get_tasks_successed(self):
        """Delete successed tasks"""
        tasks = self.task_manager.get_successed_tasks()
        return tasks

    def get_tasks_failed(self):
        """Delete successed tasks"""
        tasks = self.task_manager.get_failed_tasks()
        return tasks

    def get_task_by_id(self, aid):
        """get task object by task id"""
        for atask in self.get_tasks():
            if atask.id == aid:
                return atask
        return None

    def new_task(self, options):
        """Create a new download task"""
        if "output_dir" not in options:
            options["output_dir"] = self.output_dir

        urls = options["urls"].splitlines()
        del options["urls"]

        try:
            # have to add to database in order to get a valid task id
            self.task_manager_lock.acquire()
            tasks = []
            for u in urls:
                options["url"] = u
                atask = self.task_manager.start_download(options)
                tasks.append(atask)
        finally:
            self.task_manager_lock.release()

        for atask in tasks:
            self.queue_action(self.task_manager.queue_tasks, atask)
        return tasks

    def delete_task_by_id(self, task_id):
        """Delete a task by task id"""
        atask = self.get_task_by_id(task_id)
        self.queue_action(self.task_manager.remove_tasks, atask.origin)

    def delete_tasks(self, tasks):
        """Delete successed tasks"""
        origins = [x.origin for x in tasks]
        self.queue_action(self.task_manager.remove_tasks, origins)

    def update_task_status_by_id(self, task_id, status):
        atask = self.get_task_by_id(task_id)
        if status.lower() in ["start", "queue"]:
            self.queue_action(self.task_manager.queue_tasks, atask)

    def get_log(self):
        return "\n".join(self.text_log)

def patch_wsgiref_broken_pipe_error():
    """Catch un-caught exception by simpleserver"""
    from wsgiref.handlers import BaseHandler
    finish_response = BaseHandler.finish_response

    def my_finish_response(self):
        try:
            finish_response(self)
        except BrokenPipeError as err:
            log.w(repr(err))
    BaseHandler.finish_response = my_finish_response

def setup_arg_parser(appname=None):
    if not appname:
        appname = os.path.basename(sys.argv[0])
        if appname.endswith(".py"):
            appname = appname[:len(appname)-3]

    default_config = "${{HOME}}/.config/{}/config.ini".format(appname.lower())

    import argparse
    parser = argparse.ArgumentParser(
            description="{} rewind playing song by its lyric".format(appname))
    parser.add_argument("--version", action="version",
            version=__version_id__)
    parser.add_argument("-c", "--config", type=str,
            default=default_config,
            help="the config file to load. Default: {}".format(default_config))
    parser.add_argument("-o", "--output-dir", type=str,
            help="the directory to save download data")
    parser.add_argument("-d", "--data-dir", type=str,
            help="the directory to save server data (.sqlite, cookies)")
    parser.add_argument("-s", "--server-type", type=str,
            help="the httpd server type. Default: ThreadingWSGIRef")
    parser.add_argument("-i", "--host", type=str, help="the host to bind to")
    parser.add_argument("-p", "--port", type=int, help="the port to bind to")
    parser.add_argument("-D", "--debug", action="store_true", help="debug run")

    return parser

def main(**kwargs):
    def set_stdio_encoding(enc=NATIVE):
        import codecs; stdio = ["stdin", "stdout", "stderr"]
        for x in stdio:
            obj = getattr(sys, x)
            if not obj.encoding: setattr(sys,  x, codecs.getwriter(enc)(obj))
    set_stdio_encoding()

    parser = setup_arg_parser(APPNAME)
    args = parser.parse_args()
    if args.server_type == "help":
        print("Available server types:")
        for k in sorted(bottle.server_names.keys()):
            print("  {}".format(k))
        return

    app = App(args)
    monkey_friend = MonkeyFriend(app)
    thread_monkey_patch.monkey_patch_all(monkey_friend)

    app.start()
    app.run_server()
    app.stop()
if __name__ == '__main__':
    main()

