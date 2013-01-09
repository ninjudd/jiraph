(ns flatland.jiraph.edge-delete
  (:use [flatland.jiraph.core :only [layer]]
        [flatland.jiraph.layer :only [Basic Optimized Parent get-node]]
        [flatland.jiraph.utils :only [edges-keyseq keyseq-edge-id]]
        [flatland.jiraph.wrapped-layer :only [defwrapped]]
        [flatland.retro.core :only [at-revision current-revision]]
        [flatland.useful.map :only [remove-keys update-in*]]
        [flatland.useful.fn :only [fix]]
        [flatland.useful.utils :only [adjoin]])
  (:require [flatland.jiraph.graph :as graph :refer [unsafe-txn]]))

(def ^{:dynamic true} *default-delete-layer* :id)

(defn edges-deleted?
  "Returns true if the specified node has been marked deleted."
  ([id]
     (edges-deleted? *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (:deleted (graph/get-node delete-layer id)))))

(defn delete-edges
  "Mark the specified node as deleted. This will have the effect of deleting all edges to and from
  this node for all edge-delete wrappers using the same delete layer."
  ([id]
     (delete-edges *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (graph/update-node delete-layer id adjoin {:deleted true}))))

(defn delete-edges!
  "Mutable version of delete-edges!"
  ([id]
     (delete-edges! *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (unsafe-txn
         (delete-edges delete-layer id)))))

(defn undelete-edges
  "Mark the specified node as not deleted. This will have the effect of undeleting all edges to and from
  this node for all edge-delete wrappers using the same delete layer."
  ([id]
     (undelete-edges *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (graph/update-node delete-layer id adjoin {:deleted false}))))

(defn undelete-edges!
  "Mutable version of undelete-edges!"
  ([id]
     (undelete-edges! *default-delete-layer* id))
  ([delete-layer id]
     (let [delete-layer (fix delete-layer keyword? layer)]
       (unsafe-txn
         (undelete-edges delete-layer id)))))

(defn- mark-edges-deleted [delete-layer keyseq node]
  (let [edge-deleted? (if (edges-deleted? delete-layer (first keyseq))
                        (constantly true)
                        (partial edges-deleted? delete-layer))]
    (if-let [ks (edges-keyseq keyseq)]
      (update-in* node ks remove-keys edge-deleted?)
      (if-let [edge-id (keyseq-edge-id keyseq)]
        (when-not (edge-deleted? edge-id)
          node)
        node))))

(def ^{:private true} sentinel (Object.))

(defwrapped EdgeDeletableLayer [layer delete-layer]
  Basic
  (get-node [this id not-found]
    (let [node (get-node layer id sentinel)]
      (if (= node sentinel)
        not-found
        (mark-edges-deleted delete-layer [id] node))))

  ;; TODO needs update-in-node to set a read-wrapper that knows about the delete

  Optimized
  (query-fn [this keyseq not-found f]
    (fn [& args]
      (let [node (apply graph/query-in-node* layer keyseq sentinel f args)]
        (if (= node sentinel)
          not-found
          (mark-edges-deleted delete-layer keyseq node)))))

  Parent
  (children [this]
    [:delete])
  (child [this name]
    ({:delete (at-revision delete-layer (current-revision this))} name)))

(defn make [layer delete-layer]
  (EdgeDeletableLayer. layer delete-layer))
