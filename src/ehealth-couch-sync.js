'use strict';

angular.module('ehealth-couch-sync', [])

  .factory('sync', function($sessionStorage, $q, pouchdb) {

    // Start listening to local database changes and replicate to and
    // (in the case of no call center users) from the remote couchdb database.
    // The replication from the remote database is done in two steps.
    // The first replication is done with `{live: false}` so the `complete` event will be fired.
    // Once the replication from the couchdb database is completed, `replicate` is called again
    // with `{live: true}`.

    function replicate(twoway) {
      pouchdb.listenToChanges();
      pouchdb.replicate('to', {live: true});

      var options = {};

      function startLiveSync() {
        pouchdb.onSync('complete', 'from', startLiveSync, {remove: true});
        options.live = true;
        pouchdb.replicate('from', options);
      }

      if (twoway) {
        if ($sessionStorage.lastSeq) {
          options.since = $sessionStorage.lastSeq;
        }
        pouchdb.onSync('complete', 'from', startLiveSync);
        pouchdb.replicate('from', options);
      }
    }


    // check if there are already docs in pouch
    function wasInitialized() {
      return pouchdb.info()
        .then(function(info) {
          return info.doc_count > 0;
        });
    }


    // bulk fetch docs from the changes feed and start sync when done
    function initialize(replicateTwoway, progressCallback) {
      var changes = 0;

      function change() {
        changes++;
        if (progressCallback) {
          progressCallback(changes);
        }
      }

      function complete(changes) {
        var docs = changes.results.map(function(row) { return row.doc; });
        return pouchdb.bulkDocs(docs).then(function() {
          replicate(replicateTwoway);
        });
      }

      return wasInitialized().then(function(initialized) {
        if (initialized) {
          replicate(replicateTwoway);
          return true;
        }
        return pouchdb.allChanges()
          .then(complete, null, change);
      });
    }

    return {
      initialize: initialize
    };
  })



  .factory('pouchdb', function($sessionStorage, $q, $window, SETTINGS) {

    var self = this;

    // Create the database
    var pouchdb     = new $window.PouchDB(SETTINGS.dbName);
    var remoteCouch = SETTINGS.dbUrl + SETTINGS.dbName;

    // Initialize object for storing callbacks to execute on replication events:
    // `change`, `complete`, `uptodate` or `error`.
    // Note: `complete` is not fired when using `{live: true}`.
    self.onSyncCallbacks = {
      from: {},
      to: {}
    };
    ['change', 'complete', 'uptodate', 'error'].forEach(function(event) {
      self.onSyncCallbacks.from[event] = [];
      self.onSyncCallbacks.to[event] = [];
    });

    // Store callbacks to execute on PouchDB `change` event.
    // `change` is fired for changes coming from both the local and remote database.
    self.onChangeCallbacks = [];

    // Store live changes and replication feed, to be able to cancel them or
    // to avoid setting them twice.
    self.liveChangesFeed = null;
    self.liveReplicationFeed = {
      from: null,
      to: null
    }

    // Add a callback for replication events.
    // To remove a callback from the list set `options.remove = true`.
    function onSync(event, direction, callback, options) {
      if (options && options.remove) {
        self.onSyncCallbacks[direction][event].splice(self.onSyncCallbacks[direction][event].indexOf(callback), 1);
      } else if (self.onSyncCallbacks[direction][event].indexOf(callback) === -1) { // Avoid adding the same callback twice
        self.onSyncCallbacks[direction][event].push(callback);
      }
    }

    // Add a callback for pouchdb `change` event.
    // To remove a callback from the list set `remove` = true
    function onChange(callback, remove) {
      if (remove) {
        self.onChangeCallbacks.splice(self.onChangeCallbacks.indexOf(callback), 1);
      } else if (self.onChangeCallbacks.indexOf(callback) === -1) {
        self.onChangeCallbacks.push(callback);
      }
    }

    // Start listening to pouchdb `change` event.
    // To stop listening to changes pass {cancel: true} as options.
    function listenToChanges(options) {

      function stopListening() {
        if (self.liveChangesFeed) {
          self.liveChangesFeed.cancel();
          self.liveChangesFeed = null;
        }
        self.onChangeCallbacks = [];
      }

      function startListening() {
        pouchdb.info(function(err, info) {
          self.liveChangesFeed = pouchdb.changes({
            since: info.update_seq,
            include_docs: true,
            live: true
          }).on('change', function(change) {
            self.onChangeCallbacks.forEach(function(callback) {
              callback(change);
            });
          });
        });
      }

      if (options && options.cancel) {
        return stopListening();
      }

      // Don't create a new changes feed if there is already one
      if (!self.liveChangesFeed) {
        return startListening();
      }
    }

    // To cancel replication pass {cancel: true} as options
    function replicate(direction, options) {
      var defaultOptions = {
        batch_size: 500
      };

      function stopReplication() {
        if (self.liveReplicationFeed[direction]) {
          self.liveReplicationFeed[direction].cancel();
          self.liveReplicationFeed[direction] = null;
        }
        angular.forEach(self.onSyncCallbacks[direction], function(value, key) {
          self.onSyncCallbacks[direction][key] = [];
        });
      }

      function setDefaults() {
        angular.forEach(defaultOptions, function(value, key) {
          if (!options[key]) {
            options[key] = value;
          }
        });
      }

      function startReplication() {
        var rep = pouchdb.replicate[direction](remoteCouch, options);

        Object.keys(self.onSyncCallbacks[direction]).forEach(function(event) {
          rep.on(event, function(res) {
            if (self.onSyncCallbacks[direction][event].length > 0) {
              self.onSyncCallbacks[direction][event].forEach(function(callback) {
                callback(res);
              });
            }
          });
        });

        if (options && options.live) {
          self.liveReplicationFeed[direction] = rep;
        }
        return rep;
      }

      if (options && options.cancel) {
        return stopReplication();
      }

      // Don't start live replication twice
      if (options && options.live && self.liveReplicationFeed[direction]) {
        return self.liveReplicationFeed[direction];
      }

      setDefaults();
      return startReplication();
    }

    function cancelAllFeeds() {
      replicate('from', {cancel: true});
      replicate('to', {cancel: true});
      listenToChanges({cancel: true});
    }

    function queryView(view, options) {
      var d = $q.defer();

      pouchdb.query(view, options)
      .then(function(result) {
          d.resolve(result.rows);
        })
        .catch(function(err) {
          d.reject(err);
        });

      return d.promise;
    }

    function get(id) {
      var d = $q.defer();

      pouchdb.get(id)
        .then(function(result) {
          d.resolve(result);
        })
        .catch(function(err) {
          d.reject(err);
        });

      return d.promise;
    }

    function put(obj) {
      var d = $q.defer();

      pouchdb.put(obj)
        .then(function(result) {
          obj._rev = result.rev;
          d.resolve(obj);
        })
        .catch(function(err) {
          d.reject(err);
        });

      return d.promise;
    }

    function post(obj) {
      var d = $q.defer();

      pouchdb.post(obj)
        .then(function(result) {
          obj._id = result.id;
          obj._rev = result.rev;
          d.resolve(obj);
        })
        .catch(function(err) {
          d.reject(err);
        });

      return d.promise;
    }

    function allChanges() {
      var deferred = $q.defer();
      var changes = 0;

      var opts = {
        include_docs: true,
        batch_size: SETTINGS.dbBatchSize
      };

      function error(reason) {
        deferred.reject(reason);
      }

      function change() {
        changes++;
        deferred.notify(changes);
      }

      function complete(result) {
        $sessionStorage.lastSeq = result.last_seq;
        deferred.resolve(result);
      }

      var db = new $window.PouchDB(remoteCouch);
      db.changes(opts)
        .on('error', error)
        .on('change', change)
        .on('complete', complete);

      return deferred.promise;
    }

    function bulkDocs(docs) {
      var deferred = $q.defer();

      function success(result) {
        deferred.resolve(result);
      }

      function error(reason) {
        deferred.reject(reason);
      }

      pouchdb.bulkDocs(docs, {new_edits: false})
        .then(success)
        .catch(error);

      return deferred.promise;
    }

    function info() {
      return pouchdb.info();
    }

    return {
      queryView:       queryView,
      get:             get,
      put:             put,
      post:            post,
      replicate:       replicate,
      listenToChanges: listenToChanges,
      cancelAllFeeds:  cancelAllFeeds,
      onSync:          onSync,
      onChange:        onChange,
      allChanges:      allChanges,
      bulkDocs:        bulkDocs,
      info:            info
    };
  });
