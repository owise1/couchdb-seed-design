var objmap = require('object-map');
var objfilter = require('object-filter');
var objkeysmap = require('object-keys-map');
var objmaptoarr = require('object-map-to-array');
var objsome = require('object-some');

function addDesign(s) {
  return '_design/' + s;
}

function normalizeDoc(doc, id) {
  var result = {
    _id: id || doc._id,
    _rev: doc._rev,
    views: (doc.views && objmap(doc.views, normalizeView)) || {},
    updates: (doc.updates && objmap(doc.updates, normalizeUpdate)) || {},
    indexes: (doc.indexes && objmap(doc.indexes, normalizeIndex)) || {}
  };
  if (doc.validate_doc_update) {
    result.validate_doc_update = doc.validate_doc_update;
  }
  return result;
}

function normalizeUpdate(update) {
  return update.toString();
}

function updatesEqual(a, b) {
  return !objsome(a, function (v, k) {
    return v !== b[k];
  });
}

function normalizeView(view) {
  var r = {};

  if (typeof view === 'function' || typeof view === 'string') {
    return { map: view.toString() };
  }

  // Make sure that functions are stringified.
  if (view.map) {
    r.map = view.map.toString();
  }

  if (view.reduce) {
    r.reduce = view.reduce.toString();
  }

  if (view.index) {
    r.index = view.index.toString();
  }

  return r;
}

function normalizeIndex(index) {
  var r = {};

  if (typeof index === 'function' || typeof index === 'string') {
    return { index : index.toString() };
  }

  if (index.index) {
    r.index = index.index.toString();
  }

  if (index.analyzer) {
    r.analyzer = index.analyzer.toString();
  }

  return r;
}

function viewEqual(a, b) {
  return b && a.map === b.map && a.reduce === b.reduce;
}

function viewsEqual(a, b) {
  return !objsome(a, function (v, k) {
    return !viewEqual(v, b[k]);
  });
}

function indexEqual(a, b) {
  return b && a.index === b.index && a.analyzer === b.analyzer;
}

function indexesEqual(a, b) {
  return !objsome(a, function (v, k) {
    return !indexEqual(v, b[k]);
  });
}

function docEqual(local, remote) {
  if (!remote) {
    return false;
  }

  return viewsEqual(local.views, remote.views) &&
         indexesEqual(local.indexes, remote.indexes) &&
         updatesEqual(local.updates, remote.updates);
}

module.exports = function (db, design, cb) {
  if (!db || !design) {
    throw new TypeError('`db` and `design` are required');
  }

  var local = objmap(objkeysmap(design, addDesign), normalizeDoc);

  db.fetch({ keys: Object.keys(local) }, function (err, docs) {
    var diff;
    var remote = {};
    var update;

    if (err) {
      return cb && cb(err);
    }

    docs.rows.forEach(function (doc) {
      if (doc.doc) {
        remote[doc.key] = normalizeDoc(doc.doc);
      }
    });

    update = objmaptoarr(objfilter(local, function (value, key) {
      return !docEqual(value, remote[key]);
    }), function (v, k) {
      if (remote[k]) {
        v._rev = remote[k]._rev;
      }

      return v;
    });

    if (update.length === 0) {
      return cb && cb(null, false);
    }

    db.bulk({ docs: update }, function (err) {
      cb && cb(err, true);
    });
  });
};
