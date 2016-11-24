const TaskKeys = [
  "id",
  "origin",
  "options",
  "priority",
  "title",
  "filepath",
  "success",
  "total_size", "size_msg",
  "received",
  "playlist",
  "status",
  "percent", "percent_msg",
  "speed", "speed_msg"
];

const COLS = {
  "id": 1,
  "title": 2,
  "size": 3,
  "progress": 4,
  "speed": 5,
  "origin": 6
};

const ATTR2COLS = {
  "id": "id",
  "title": "title",
  "size_msg": "size",
  "percent_msg": "progress",
  "speed_msg": "speed",
  "origin": "origin"
};

// clean task data for later parse
function enhance_task_data(task_data) {
  var td = task_data;
  var haskey = td.hasOwnProperty;
  var percent;
  var msg;

  if (td.total_size > 0) {
    msg = "{0}".format_me(num2human(td.total_size));
  } else {
    msg = "-";
  }
  td["size_msg"] = msg;

  // percent
  if (td.success > 0) {
    msg = "Done";
  } else if (td.status !== "Start") {
    msg = td.status;
  }else if (td.total_size > 0) {
    percent = td.received * 100 / td.total_size;
    msg = "{0}%".format_me(percent.toFixed(2));
    td["percent"] = percent;
  } else {
    msg = "-";
  }
  td["percent_msg"] = msg;

  // speed
  if (td.status !== "Start") {
    msg = "-";
  } else if (typeof td.percent !== 'undefined' && td.speed >= 0) {
    msg = "{0}/s".format_me(num2human(td.speed));
  } else {
    msg = "-";
  }

  td["speed_msg"] = msg;
  return td;
}

// Task Object
function Task(atask) {
  var i;
  if (typeof atask !== "undefined") {
    atask = enhance_task_data(atask);
    for (i = 0; i < TaskKeys.length; i++) {
      var k = TaskKeys[i];
      this[k] = atask[k];
    }
  }
}

// Test if a task is changed from the given task data
Task.prototype.changed = function(atask) {
  var changed_keys = [];
  for (var i = 0; i < TaskKeys.length; i++) {
    var k = TaskKeys[i];
    if (typeof atask[k] !== "undefined" && this[k] !== atask[k]) {
      changed_keys.push(k);
    }
  }
  return changed_keys;
};

Task.prototype.update = function(atask) {
  atask = enhance_task_data(atask);
  var changed = this.changed(atask);
  if (changed.length <= 0) {
    return;
  }
  var $tr = $('tr[data-id="' + this.id + '"]');
  var col_elms = $('tr[data-id="' + this.id + '"] > td');

  if (!!changed.title) {
  }

  for (var i = 0; i < changed.length; i++) {
    var k = changed[i];
    var value = atask[k];
    this[k] = value;

    if (ATTR2COLS.hasOwnProperty(k)) {
      var idx = COLS[ATTR2COLS[k]];
      value = this[k];
      if (k === "title") {
        var value = gen_title_html(this.id, this.title, this.origin);
        col_elms.eq(idx).html(value);
      } else {
        col_elms.eq(idx).text(value);
      }
    } else if (k == "success") {
      var class_v = "";
      if (value > 0) {
        class_v = "success";
      } else if (value < 0) {
        class_v = "danger";
      }
      $tr.removeClass("success danger info").addClass(class_v);
    } else if (k == "status") {
      if (value == "Start" || value == "Queue") {
        $tr.removeClass("success danger").addClass("info");
      }
    }
  }
};

function gen_title_html(id, title, url) {
  var hostname = $('<a>').prop('href', url).prop('hostname');
  var favicon_url = 'http://{0}/favicon.ico'.format_me(hostname);
  var icon_msg = '<a href="{0}" class="favicon" title="{0}">\
    <img src="{1}"></a>'.format_me(url, favicon_url);
  var html_content = '{0} {1} <a\
    href="#" data-id="{2}" data-toggle="popover" placement="auto bottom"\
             class="task-popover has-popover" data-content="over my body"\
             data-html="true">&raquo;</a>'.format_me(icon_msg, title, id);
  return html_content;
}

Task.prototype.attach = function() {
  var title_html = gen_title_html(this.id, this.title, this.origin);
  var tr_content = '\
    <tr data-id="{0}">\
    <td>\
    <input type="checkbox" name="check-task" id="check-task-{0}">\
    </td>\
    <td>{0}</td>\
    <td>{1}</td>\
    <td class="text-right">{2}</td>\
    <td class="text-right">{3}</td>\
    <td class="text-right">{4}</td>\
    </tr>'.format_me(
        this.id,
        title_html,
        this.size_msg,
        this.percent_msg,
        this.speed_msg
        );
  $('#task-table > tbody').append(tr_content);
};

Task.prototype.detach = function() {
  var $tr = $("tr[data-id='" + this.id + "']");
  $tr.remove();
};

// TaskManager Object
function TaskManager() {
  this.tasks = {};
  return this;
}

TaskManager.prototype.has_id = function(aid) {
  return this.tasks.hasOwnProperty(aid);
};

TaskManager.prototype.add = function(task_datas) {
  for (var id in task_datas) {
    if (!task_datas.hasOwnProperty(id)) {
      continue;
    }
    if (this.tasks.hasOwnProperty(id)) {
      this.tasks[id].update(task_datas[id]);
    } else {
      this.new_task(task_datas[id]);
    }
  }
};

TaskManager.prototype.drop = function(atask_id) {
  // drop from local task_manager
  var atask = this.tasks[atask_id];
  if (typeof atask !== undefined) {
    atask.detach();
    delete this.tasks[atask_id];
    return atask;
  }
};

TaskManager.prototype.new_task = function(task_data_json) {
  var atask = new Task(task_data_json);
  this.tasks[atask.id] = atask;
  atask.attach();
};

TaskManager.prototype.update_a_task = function(task_data_json) {
  var task_data = task_data_json;
  var atask;
  if (this.has_id(task_data.id)) {
    atask = this.tasks[task_data.id];
    atask.update(task_data);
  } else {
    this.new_task(task_data);
  }
};

TaskManager.prototype.update_tasks = function(task_data_json_list) {
  var datals = task_data_json_list;
  if (!datals) {
    return;
  }
  for (var key in datals) {
    if (!datals.hasOwnProperty(key)) {
      continue;
    }
    var task_data = datals[key];
    //console.log("update_tasks", task_data);
    this.update_a_task(task_data);
  }
};

TaskManager.prototype.remove_successed = function() {
  var task_ids = [];
  for (var key in this.tasks) {
    if (!this.tasks.hasOwnProperty(key)) {
      continue;
    }
    if (this.tasks[key].success > 0) {
      task_ids.push(this.tasks[key].id);
    }
  }
  this.remove_task_list(task_ids);
};

TaskManager.prototype.remove_failed = function() {
  var task_ids = [];
  for (var key in this.tasks) {
    if (!this.tasks.hasOwnProperty(key)) {
      continue;
    }
    if (this.tasks[key].success < 0) {
      task_ids.push(this.tasks[key].id);
    }
  }
  this.remove_task_list(task_ids);
};

TaskManager.prototype.remove_all = function() {
  var task_ids = [];
  for (var key in this.tasks) {
    if (!this.tasks.hasOwnProperty(key)) {
      continue;
    }
    task_ids.push(this.tasks[key].id);
  }
  this.remove_task_list(task_ids);
};

TaskManager.prototype.get_selected = function() {
  var task_ids = [];
  for (var key in this.tasks) {
    if (!this.tasks.hasOwnProperty(key)) {
      continue;
    }

    var $checkbox = $("#check-task-" + this.tasks[key].id);
    if ($checkbox.prop("checked")) {
      task_ids.push(this.tasks[key].id);
    }
  }
  return task_ids;
};

TaskManager.prototype.remove_selected = function() {
  var task_ids = this.get_selected();
  this.remove_task_list(task_ids);
};

TaskManager.prototype.remove_task_list = function(task_ids) {
  var url = "/api/ver1/tasks";
  if (task_ids.length <= 0) {
    return;
  }

  var data = {ids: task_ids};
  ajax_json(url, data, "DELETE");

  for (var i = 0; i < task_ids.length; i++) {
    this.drop(task_ids[i]);
  }
};

TaskManager.prototype.start_selected = function() {
  var task_ids = this.get_selected();
  var base_url = "/api/ver1/tasks";
    console.log("start", base_url, task_ids)
  for (var i = 0; i < task_ids.length; i++) {
      var data = {"status": "Queue"};
      var url = '{0}/{1}/status'.format_me(base_url, task_ids[i]);
      console.log(url);
      ajax_json(url, data, "PUT");
  }
};

TaskManager.prototype.do_cmd = function(cmd_id) {
  if (cmd_id == "task-start-selected") {
    this.start_selected();
  } else if (cmd_id == "task-remove-failed") {
    this.remove_failed();
  } else if (cmd_id == "task-remove-successed") {
    this.remove_successed();
  } else if (cmd_id == "task-remove-all") {
    var url = "/api/ver1/tasks";
    this.remove_all();
  } else if (cmd_id == "task-remove-selected") {
    this.remove_selected();
  }
};

TaskManager.prototype.set_popover_content = function(anchor) {
  var task_id = anchor.data("id");
  var atask = this.tasks[task_id];
  var data = $.extend({}, atask, atask.options);
  var playlist_msg = "";
  if (data.playlist) {
    playlist_msg = '\
     Files: ' + data.playlist.join("\n" + Array(12).join(" ")) + "\n";
  }
  var pop_msg = '<pre>\
      File: {0}\n\
     Total: {1}\n\
  Received: {2}\n\
\n\
    Format: {3}\n\
  Playlist: {4}\n\
    XProxy: {5}\n\
    Origin: {6}\n{7}\
    </pre>'.format_me(data.filepath, num2human(data.total_size),
    num2human(data.received), data.stream_id, data.do_playlist,
    data.extractor_proxy, data.origin, playlist_msg);
  anchor.attr("data-content", pop_msg);
};


function TaskUpdater(taskman) {
  this.last_modified = null;
  this.taskman = taskman;
  this.error_retry_timeout = ERROR_RETRY_TIMEOUT;
}

TaskUpdater.prototype.update_server_sent_event = function() {
    var url = "/api/ver1/tasks/sse";
    var source = new EventSource(url);
    var self = this;

    //console.log("Binding event source");

    source.onmessage = function(e) {console.log(e.data)};
    source.addEventListener('tasks', function(e) {
      var tasks = JSON.parse(e.data);
      //console.log(e.data);
      //console.log("sse", tasks);
      //console.log("tasks event", self);
      self.taskman.update_tasks(tasks);
    }, false);

    source.addEventListener('open', function(e) {
      //console.log("open:", e, self);
      if (self.error_retry_timeout !== ERROR_RETRY_TIMEOUT) {
        self.error_retry_timeout = ERROR_RETRY_TIMEOUT; // minutes
      }
    }, false);

    source.addEventListener('error', function(e) {
      source.close();
      setTimeout(function() {
          self.update_server_sent_event();
        }, self.error_retry_timeout);
      self.error_retry_timeout *= 2;
    }, false);
};

TaskUpdater.prototype.update_query_tasks = function() {
  var url = "/api/ver1/tasks";
  var headers = {};
  if (this.last_modified !== null) {
    headers["If-Modified-Since"] = this.last_modified;
  }

  var self = this;
  $.ajax(url, {
    method: "GET",
    dataType: "json",
    headers: headers,
    success: function(task_ls, text_status, jq_xhr) {
      if (jq_xhr.status == 304) {
        return;
      }

      self.taskman.update_tasks(task_ls);
      self.last_modified = jq_xhr.getResponseHeader("Date");
      }
    }).always(function() {
    setTimeout(function() {
      self.update_query_tasks();
    }, TASKS_UPDATE_TIMEOUT);
    });
};

function ajax_json(url, data_dict, rtype, func) {
  return $.ajax({
    type: rtype,
    url: url,
    data: JSON.stringify(data_dict),
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    success: func
  });
}

// format a string with {0}, {1} ...
String.prototype.format_me = function() {
    var newStr = this;

    for (var i = 0; i < arguments.length; i++) {
        newStr = newStr.split('{' + i + '}').join(arguments[i]);
    }
    return newStr;
};
// Convert integer to human readable number
// http://stackoverflow.com/a/10469752
function num2human(num, base) {
  if (num === 0) {
    return '0B';
  }
  var base = typeof base !== 'undefined' ? base : 1000;
  var s = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
  var e = Math.floor(Math.log(num) / Math.log(base));
  return (num / Math.pow(base, e)).toFixed(2) + "" + s[e];
}

// vim:fileencoding=utf-8:sw=2:et
