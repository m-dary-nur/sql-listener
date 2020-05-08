var zongji = require("zongji");
var EventEmitter = require("events").EventEmitter;
var util = require("util");

function zongjiManager(connection, options, onBinlog) {
   var instance = new zongji(connection, options);

   instance.on("error", function (reason) {
      instance.removeListener("binlog", onBinlog);
      setTimeout(function () {
         // If multiple errors happened, a new instance may have already been created
         if (!("child" in instance)) {
            instance.child = zongjiManager(
               connection,
               {
                  ...options,
                  binlogName: instance.binlogName,
                  binlogNextPos: instance.binlogNextPos,
               },
               onBinlog
            );
            instance.emit("child", instance.child, reason);
            instance.child.on("child", (child) => instance.emit("child", child));
         }
      }, RETRY_TIMEOUT);
   });
   instance.on("binlog", onBinlog);
   instance.start(options);
   return instance;
}

function SqlListener(connection, debug = false, zOptions = {}) {
   EventEmitter.call(this);

   this._zongji_opt = {
      startAtEnd: true,
      includeSchema: {},
      excludeSchema: {},
      includeEvents: ["rotate", "tablemap", "writerows", "updaterows", "deleterows"],
      ...zOptions,
   };

   var zongji = zongjiManager(connection, this._zongji_opt, this._event.bind(this));

   this._zongji = zongji;

   zongji.on("child", function (child, reason) {
      console.log("New Instance Created", reason);
      this._zongji.stop();
      this._zongji = child;
   });

   process.on("SIGTERM", this._term.bind(this));
   process.on("SIGINT", this._int.bind(this));

   this._debug = debug;
   if (this._debug) console.log("SqlListener Instance spawned.");
}

util.inherits(SqlListener, EventEmitter);

//==================================================================//

SqlListener.prototype.listen = function (database, tableList = true) {
   if (this._debug) console.log("Subscribed to %s", database);
   this._zongji_opt.includeSchema[database] = tableList;
};

SqlListener.prototype.unlisten = function (database) {
   if (this._debug) console.log("Unsubscribed to %s", database);
   if (database in this._zongji_opt.includeSchema) {
      delete this._zongji_opt.includeSchema[database];
   }
};

SqlListener.prototype.start = function () {
   if (this._debug) console.log("Starting SqlListener.");
   return this._zongji.start(this._zongji_opt);
};

SqlListener.prototype.stop = function () {
   if (this._debug) console.log("Stopping SqlListener.");
   return this._zongji.stop();
};

SqlListener.prototype._term = function () {
   if (this._debug) console.log("Got SIGTERM, Received shutdown signal, closing connections");
   this.stop();
   process.exit(0);
};

SqlListener.prototype._int = function () {
   if (this._debug) console.log("Got SIGINT, Received shutdown signal, closing connections");
   this.stop();
   process.exit(0);
};

SqlListener.prototype._event = function (e) {
   var evt = new Event(e);
   if (evt.event() !== "rotate" && evt.event() !== "tablemap") {
      var database = evt.database();
      var table = evt.table();
      var event = evt.event();

      if (this._debug) console.log("Received binlog event [%s] on [%s.%s]", event, database, table);

      this.emit(database, evt);
   } else {
      console.log("event rotate emitted");
   }
};

//==================================================================//

function Event(e) {
   this._e = e;
}

Event.prototype.event = function () {
   return this._e.getEventName().replace("rows", "").replace("write", "insert");
};

Event.prototype.database = function () {
   return this._e.tableMap[this._e.tableId].parentSchema;
};

Event.prototype.table = function () {
   return this._e.tableMap[this._e.tableId].tableName;
};

Event.prototype.rows = function (columnExclude = null, as = null) {
   if (!("rows" in this._e)) return [];
   const rows = this.event() === "update" ? this._e.rows.map((x) => x.after) : this._e.rows;
   return columnExclude
      ? {
           [as ? as : columnExclude]: rows[0][columnExclude],
           data: rows,
        }
      : rows;
};

Event.prototype.rowsDiff = function (columnExclude = null, as = null, ...columnsInclude) {
   if (!("rows" in this._e)) return [];
   var diff = function (before, after) {
      var obj = {};
      for (var i in after) {
         if (!before.hasOwnProperty(i) || after[i] !== before[i]) {
            obj[i] = after[i];
         }
      }
      return obj;
   };
   const getRow = this.event() === "update" ? this._e.rows[0].after : this._e.rows[0];
   const rowsDiff =
      this.event() === "update"
         ? this._e.rows.map((x) => {
              if (columnsInclude.length > 0) {
                 let obj = {};
                 columnsInclude.map((col) => (obj[col] = x[col]));
                 return ({ ...obj, ...diff(x.before, x.after);})
              }
              return diff(x.before, x.after);
           })
         : this._e.rows;
   return columnExclude
      ? {
           [as ? as : columnExclude]: getRow[columnExclude],
           data: rowsDiff,
        }
      : rowsDiff;
};

module.exports = SqlListener;
