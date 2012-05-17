(ns jiraph.delete
  (:use jiraph.layer retro.core
        [jiraph.core :only [layer]]
        [jiraph.utils :only [meta-keyseq? edges-keyseq deleted-edge-keyseq deleted-node-keyseq]]
        [useful.map :only [map-vals-with-keys update update-in*]]
        [useful.fn :only [fix fixing]]
        [useful.utils :only [adjoin]]
        [useful.datatypes :only [assoc-record]])
  (:require [jiraph.graph :as graph]))

(declare node-deleted?)

(def ^{:dynamic true} *default-delete-layer-name* :id)

(letfn [(exists?  [delete-layer id exists]  (and exists (not (node-deleted? delete-layer id))))
        (deleted? [delete-layer id deleted] (or deleted (node-deleted? delete-layer id) deleted))]

  (defn mark-edges-deleted [delete-layer keyseq node]
    (if-let [ks (edges-keyseq keyseq)]
      (update-in* node ks fixing map? map-vals-with-keys
                  (if (meta-keyseq? keyseq)
                    (partial exists? delete-layer)
                    (fn [id edge]
                      (update edge :deleted (partial deleted? delete-layer id)))))
      (if-let [ks (deleted-edge-keyseq keyseq)]
        (update-in* node ks
                    (if (meta-keyseq? keyseq)
                      (partial exists?  delete-layer (second keyseq))
                      (partial deleted? delete-layer (first  keyseq))))
        node)))

  (defn mark-deleted [delete-layer keyseq node]
    (->> (if-let [ks (deleted-node-keyseq keyseq)]
           (update-in* node ks (partial deleted? delete-layer (first keyseq)))
           node)
         (mark-edges-deleted delete-layer keyseq))))

(defn node-deleted?
  "Returns true if the specified node has been deleted."
  ([id]
     (node-deleted? *default-delete-layer-name* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (:deleted (graph/get-node delete-layer id)))))

(defn delete-node
  "Functional version of delete-node!"
  [delete-layer id]
  (graph/update-node delete-layer id adjoin {:deleted true}))

(defn delete-node!
  "Mark the specified node as deleted."
  ([id]
     (delete-node! *default-delete-layer-name* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (dotxn delete-layer
         (delete-node delete-layer id)))))

(defn undelete-node
  "Functional version of undelete-node!"
  [delete-layer id]
  (graph/update-node delete-layer id adjoin {:deleted false}))

(defn undelete-node!
  "Mark the specified node as not deleted."
  ([id]
     (undelete-node! *default-delete-layer-name* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (dotxn delete-layer
         (undelete-node delete-layer id)))))

(def ^{:private true} sentinel (Object.))

(defrecord DeletableLayer [layer delete-layer]
  Object
  (toString [this] (pr-str this))

  Enumerate
  (node-id-seq [this] (node-id-seq layer))
  (node-seq    [this] (node-seq layer))

  Basic
  (get-node [this id not-found]
    (let [node (get-node layer id sentinel)]
        (if (= node sentinel)
          not-found
          (mark-deleted delete-layer [id] node))))

  (assoc-node!  [this id attrs] (assoc-node! layer id attrs))
  (dissoc-node! [this id]       (dissoc-node! layer id))

  Optimized
  (query-fn [this keyseq not-found f]
    (fn [& args]
      (let [node (apply graph/query-in-node* layer keyseq sentinel f args)]
        (if (= node sentinel)
          not-found
          (mark-deleted delete-layer keyseq node)))))

  (update-fn [this keyseq f] (update-fn layer keyseq f))

  Layer
  (open      [this] (open  layer))
  (close     [this] (close layer))
  (sync!     [this] (sync! layer))
  (optimize! [this] (optimize! layer))
  (truncate! [this] (truncate! layer))

  Schema
  (schema      [this id]       (schema layer id))
  (verify-node [this id attrs] (verify-node layer id attrs))

  ChangeLog
  (get-revisions   [this id]  (get-revisions layer id))
  (get-changed-ids [this rev] (get-changed-ids layer rev))

  WrappedTransactional
  (txn-wrap [this f]
    (let [wrapped (txn-wrap layer ; let layer wrap transaction, but call f with this
                            (fn [_]
                              (f this)))]
      (fn [^DeletableLayer layer]
        (wrapped (.layer layer)))))

  Revisioned
  (at-revision      [this rev] (assoc-record this :layer (at-revision layer rev)))
  (current-revision [this]     (current-revision layer))

  OrderedRevisions
  (max-revision [this] (max-revision layer))

  Preferences
  (manage-changelog? [this] (manage-changelog? layer))
  (manage-incoming?  [this] (manage-incoming?  layer))
  (single-edge?      [this] (single-edge?      layer)))

(defn deletable-layer [layer delete-layer]
  (DeletableLayer. layer delete-layer))