#!/usr/bin/python3
# vim:fileencoding=utf-8:sw=4:et
"""
Monkey patch modules to make them thread aware.

Since `urllib` is not thread safe, .common might not be
thread safe before and after monkey patched.

Example:
    import thread_monkey_patch
    class MonkeyFriend(thread_monkey_patch.UIFriend):
        def __init__(self, app):
            self.app = app
        def sprint(self, text, *color):
            self.app.print_log(text, *color)

    class MyTask(thread_monkey_patch.TaskBase):
        def __init__(self, app):
            self.app = app

        def target(self, *args, **kwargs):
            self.app.do_my_work(*args, **kwargs)

        def update_task_status(self, urls=None, title=None,
                file_path=None, progress_bar=None):
            self.app.update_something(blah, blah__)

    main_app = MyApp()
    monkey_friend = MonkeyFriend(main_app)

    thread_monkey_patch.monkey_patch_all(monkey_friend)

"""

import sys
import os
import abc
import time
import socket
import threading
import urllib.request as request

from . import common
from .common import tr, urls_size, get_filename, url_save, url_save_chunked

Origins = {} # kept original functions
UI_Monkey = None

class MyLocal(threading.local):
    """Thread local specific "global" variable"""
    url_opener = None
Thread_Local = MyLocal()

class UIFriend:
    """Interface for UI object used by the UI_Monkey class.
    Override methods to acquire corresponding functionality in UI_Monkey
    """
    __metaclass__ = abc.ABCMeta

    def sprint(self, text, *colors):
        return text

class TaskBase:
    """Interface to dowload thread task
    normally, the following methods need to be override:

        def update_task_status(self, urls=None, title=None,
                file_path=None, progress_bar=None):
            pass

        def target(self, *args, **kwargs):
            pass
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def update_task_status(self, urls=None, title=None,
            file_path=None, progress_bar=None):
        """Called by the download_urls function in a download thread
        Setup download status of the given task
        """
        pass

    @abc.abstractmethod
    def target(self, *args, **kwargs):
        """Target to run by the task thread in start() method"""
        pass

    def pre_thread_start(self, athread):
        """Called before a thread is created but not start"""
        pass

    def post_thread_start(self, athread):
        """Called after a thread is created and started"""
        pass

    def start(self, thread_target=None, *args, **kwargs):
        """call this method to start the task"""
        if thread_target is None:
            thread_target = self.target

        t = threading.Thread(target=thread_target,
                args=args, kwargs=kwargs)

        self.thread = t
        t.download_task = self
        t.daemon = True

        self.pre_thread_start(t)
        t.start()
        self.post_thread_start(t)

        time.sleep(0.1)

    def stop(self):
        """Somehow stop the task"""
        # we cannot stop a thread for now
        pass

def install_ui_monkey(ui_obj):
    """Supply a GUI object to UIMonkey"""
    global UI_Monkey
    if UI_Monkey is None:
        UI_Monkey = UIMonkey(ui_obj)

# common.py
def thread_download_urls(urls, title, ext, total_size, output_dir='.', refer=None, merge=True, faker=False):
    """download_urls() which register progress bar to taskManager
    A `download_task` attribute was attached to the current thread object.
    Download information should be passed through the
    `download_task.update_task_status()` method.

    """
    assert urls
    force=False

    if not total_size:
        try:
            total_size = urls_size(urls)
        except:
            import traceback
            import sys
            traceback.print_exc(file = sys.stdout)
            pass

    title = tr(get_filename(title))

    filename = '%s.%s' % (title, ext)
    filepath = os.path.join(output_dir, filename)

    bar = SimpleProgressBar(total_size, len(urls))

    thread_me = threading.current_thread()
    #print("download task of current thread:", thread_me.download_task)
    try:
        thread_me.download_task.update_task_status(urls=urls,
                file_path=filepath, progress_bar=bar)
    except AttributeError:
        pass

    if total_size:
        if not force and os.path.exists(filepath) and os.path.getsize(filepath) >= total_size * 0.9:
            print('Skipping %s: file already exists' % filepath)
            print()
            bar.done()
            return

    if len(urls) == 1:
        url = urls[0]
        print('Downloading %s ...' % tr(filename))
        url_save(url, filepath, bar, refer = refer, faker = faker)
        bar.done()
    else:
        parts = []
        print('Downloading %s.%s ...' % (tr(title), ext))
        for i, url in enumerate(urls):
            filename = '%s[%02d].%s' % (title, i, ext)
            filepath = os.path.join(output_dir, filename)
            parts.append(filepath)
            #print 'Downloading %s [%s/%s]...' % (tr(filename), i + 1, len(urls))
            bar.update_piece(i + 1)
            url_save(url, filepath, bar, refer = refer, is_part = True, faker = faker)
        bar.done()

        if not merge:
            print()
            return
        if ext in ['flv', 'f4v']:
            try:
                from .processor.ffmpeg import has_ffmpeg_installed
                if has_ffmpeg_installed():
                    from .processor.ffmpeg import ffmpeg_concat_flv_to_mp4
                    ffmpeg_concat_flv_to_mp4(parts, os.path.join(output_dir, title + '.mp4'))
                else:
                    from .processor.join_flv import concat_flv
                    concat_flv(parts, os.path.join(output_dir, title + '.flv'))
            except:
                raise
            else:
                for part in parts:
                    os.remove(part)

        elif ext == 'mp4':
            try:
                from .processor.ffmpeg import has_ffmpeg_installed
                if has_ffmpeg_installed():
                    from .processor.ffmpeg import ffmpeg_concat_mp4_to_mp4
                    ffmpeg_concat_mp4_to_mp4(parts, os.path.join(output_dir, title + '.mp4'))
                else:
                    from .processor.join_mp4 import concat_mp4
                    concat_mp4(parts, os.path.join(output_dir, title + '.mp4'))
            except:
                raise
            else:
                for part in parts:
                    os.remove(part)

        else:
            print("Can't merge %s files" % ext)

    print()

def thread_download_urls_chunked(urls, title, ext, total_size, output_dir='.', refer=None, merge=True, faker=False):
    """download_urls_chunked() which register progress bar to taskManager"""
    assert urls
    force = False

    assert ext in ('ts')

    title = tr(get_filename(title))

    filename = '%s.%s' % (title, 'ts')
    filepath = os.path.join(output_dir, filename)

    bar = SimpleProgressBar(total_size, len(urls))
    thread_me = threading.current_thread()
    try:
        thread_me.download_task.update_task_status(urls=urls,
                file_path=filepath, progress_bar=bar)
    except AttributeError:
        pass

    if total_size:
        if not force and os.path.exists(filepath[:-3] + '.mkv'):
            print('Skipping %s: file already exists' % filepath[:-3] + '.mkv')
            print()
            bar.done()
            return

    if len(urls) == 1:
        parts = []
        url = urls[0]
        print('Downloading %s ...' % tr(filename))
        filepath = os.path.join(output_dir, filename)
        parts.append(filepath)
        url_save_chunked(url, filepath, bar, refer = refer, faker = faker)
        bar.done()

        if not merge:
            print()
            return
        if ext == 'ts':
            from .processor.ffmpeg import has_ffmpeg_installed
            if has_ffmpeg_installed():
                from .processor.ffmpeg import ffmpeg_convert_ts_to_mkv
                if ffmpeg_convert_ts_to_mkv(parts, os.path.join(output_dir, title + '.mkv')):
                    for part in parts:
                        os.remove(part)
                else:
                    os.remove(os.path.join(output_dir, title + '.mkv'))
            else:
                print('No ffmpeg is found. Conversion aborted.')
        else:
            print("Can't convert %s files" % ext)
    else:
        parts = []
        print('Downloading %s.%s ...' % (tr(title), ext))
        for i, url in enumerate(urls):
            filename = '%s[%02d].%s' % (title, i, ext)
            filepath = os.path.join(output_dir, filename)
            parts.append(filepath)
            #print 'Downloading %s [%s/%s]...' % (tr(filename), i + 1, len(urls))
            bar.update_piece(i + 1)
            url_save_chunked(url, filepath, bar, refer = refer, is_part = True, faker = faker)
        bar.done()

        if not merge:
            print()
            return
        if ext == 'ts':
            from .processor.ffmpeg import has_ffmpeg_installed
            if has_ffmpeg_installed():
                from .processor.ffmpeg import ffmpeg_concat_ts_to_mkv
                if ffmpeg_concat_ts_to_mkv(parts, os.path.join(output_dir, title + '.mkv')):
                    for part in parts:
                        os.remove(part)
                else:
                    os.remove(os.path.join(output_dir, title + '.mkv'))
            else:
                print('No ffmpeg is found. Merging aborted.')
        else:
            print("Can't merge %s files" % ext)

    print()

class SimpleProgressBar:
    def __init__(self, total_size, total_pieces=1):
        self.total_size = total_size
        self.total_pieces = total_pieces
        self.current_piece = 1
        self.received = 0
        self.finished = False

    def update(self):
        pass
    def update_received(self, n):
        self.received += n
        self.update()
    def update_piece(self, n):
        self.current_piece = n
    def done(self):
        self.finished = True
        self.received = self.total_size
        self.current_piece = self.total_pieces
        self.update()

def monkey_patch_common():
    """Replace common.download_urls() with our own functions"""
    m = {}
    m["download_urls"] = common.download_urls
    m["download_urls_chunked"] = common.download_urls_chunked
    Origins["common"] = m

    common.download_urls = thread_download_urls
    common.download_urls_chunked = thread_download_urls_chunked

class UIMonkey:
    """Monkey patch functions with an UI object"""
    def __init__(self, ui_obj):
        if not isinstance(ui_obj, UIFriend):
            raise(TypeError, "parameter ui_obj is not a instance of UIFriend")
        self.ui_obj = ui_obj

    def sprint(self, text, *colors):
        ret = Origins["util.log"]["sprint"](text, *colors)

        try:
            self.ui_obj.sprint(text, *colors)
        except:
            sys.stderr.write(sys.exc_info())
            sys.stderr.flush()
        return ret

# util.log.py
def monkey_patch_log():
    from .util import log
    m = {}
    m["sprint"] = log.sprint
    Origins["util.log"] = m

    log.sprint = UI_Monkey.sprint

# urllib.request
def thread_urlopen(url, data=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
            *, cafile=None, capath=None, cadefault=False):
    """Patch urllib.request.urlopen"""
    build_opener = request.build_opener
    _have_ssl = request._have_ssl
    if _have_ssl:
        ssl = request.ssl

    _opener = Thread_Local.url_opener

    if cafile or capath or cadefault:
        if not _have_ssl:
            raise ValueError('SSL support not available')
        context = ssl._create_stdlib_context(cert_reqs=ssl.CERT_REQUIRED,
                                             cafile=cafile,
                                             capath=capath)
        https_handler = request.HTTPSHandler(context=context,
                check_hostname=True)
        opener = build_opener(https_handler)
    elif _opener is None:
        _opener = opener = build_opener()
        Thread_Local.url_opener = _opener
    else:
        opener = _opener

    #print("my-opener", opener)
    return opener.open(url, data, timeout)

def thread_build_opener(*args):
    """Add cookiejar to any opener, if common.cookies_txt available"""
    if common.cookies_txt is not None:
        cookie_handler = request.HTTPCookieProcessor(common.cookies_txt)
        args = args + (cookie_handler,)
    ret = Origins["urllib.request"]["build_opener"](*args)
    return ret

def thread_install_opener(opener):
    """Patch urllib.request.install_opener"""
    Thread_Local.url_opener = opener

def monkey_patch_urllib_request():
    """Try to make urllib.request robust by avoiding global `_opener`"""
    m = {}
    m["urlopen"] = request.urlopen
    m["build_opener"] = request.build_opener
    m["install_opener"] = request.install_opener
    Origins["urllib.request"] = m

    request.urlopen = thread_urlopen
    request.build_opener = thread_build_opener
    request.install_opener = thread_install_opener

def monkey_patch_all(gui_friend):
    """monkey patch all the patchable functions"""
    monkey_patch_urllib_request()
    monkey_patch_common()

    install_ui_monkey(gui_friend)
    monkey_patch_log()

