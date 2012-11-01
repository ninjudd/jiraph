(ns jiraph.delete
  (:use [jiraph.core :only [layer]]
        [jiraph.layer :only [Basic Optimized Parent get-node]]
        [jiraph.utils :only [meta-keyseq? edges-keyseq deleted-edge-keyseq deleted-node-keyseq]]
        [jiraph.wrapped-layer :only [NodeFilter defwrapped]]
        [retro.core :only [at-revision current-revision]]
        [useful.map :only [map-vals-with-keys update update-in*]]
        [useful.fn :only [fix fixing]]
        [useful.utils :only [adjoin]]
        [useful.datatypes :only [assoc-record]])
  (:require [jiraph.graph :as graph :refer [unsafe-txn]]))

(declare node-deleted?)

(def ^{:dynamic true} *default-delete-layer* :id)

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
     (node-deleted? *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (:deleted (graph/get-node delete-layer id)))))

(defn delete-node
  "Functional version of delete-node!"
  ([id]
     (delete-node *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (graph/update-node delete-layer id adjoin {:deleted true}))))

(defn delete-node!
  "Mark the specified node as deleted."
  ([id]
     (delete-node! *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (unsafe-txn
         (delete-node delete-layer id)))))

(defn undelete-node
  "Functional version of undelete-node!"
  ([id]
     (delete-node *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (graph/update-node delete-layer id adjoin {:deleted false}))))

(defn undelete-node!
  "Mark the specified node as not deleted."
  ([id]
     (undelete-node! *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (unsafe-txn
         (undelete-node delete-layer id)))))

(def ^{:private true} sentinel (Object.))

(defwrapped DeletableLayer [layer delete-layer]
  Basic
  (get-node [this id not-found]
    (let [node (get-node layer id sentinel)]
      (if (= node sentinel)
        not-found
        (mark-deleted delete-layer [id] node))))
  ;; TODO needs update-in-node to set a read-wrapper that knows about the delete

  Optimized
  (query-fn [this keyseq not-found f]
    (fn [& args]
      (let [node (apply graph/query-in-node* layer keyseq sentinel f args)]
        (if (= node sentinel)
          not-found
          (mark-deleted delete-layer keyseq node)))))

  NodeFilter
  (keep-node? [this id]
    (not (node-deleted? delete-layer id)))

  Parent
  (children [this]
    [:delete])
  (child [this name]
    ({:delete (at-revision delete-layer (current-revision this))} name)))

(defn deletable-layer [layer delete-layer]
  (DeletableLayer. layer delete-layer))
