(ns flatland.jiraph.forward
  (:use flatland.jiraph.wrapped-layer)
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update]]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.graph :as graph]
            [flatland.useful.utils :refer [invoke adjoin]]
            [flatland.useful.fn :refer [given]]
            [flatland.useful.seq :refer [assert-length]]
            [flatland.useful.map :refer [map-keys update]]
            [clojure.core.match :refer [match]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

(defn update-keyseq [keyseq update-id]
  (-> (vec keyseq)
      (given (and (= (second keyseq) :edges)
                  (nnext keyseq))
        (update-in [2] update-id))
      (update-in [0] update-id)))

(defn fix-update [keyseq f args canon]
  (let [[id & keys :as keyseq] (update-keyseq keyseq canon)]
    (cons keyseq
          (if (= f adjoin)
            [adjoin (let [val (first (assert-length 1 args))]
                      [(ruminate/update-edge-ids keys val canon)])]
            [(fn [& args]
               (let [orig (apply f args)]
                 (ruminate/update-edge-ids keys orig canon)))
             args]))))

(defwrapped ForwardLayer
  [layer id-transform wrap-child?]
  []

  layer/Basic
  (get-node [this id not-found]
    (let [layer (at-revision layer (current-revision this))
          canon (id-transform layer graph/get-in-node)]
      (layer/get-node layer (canon id) not-found)))
  (update-in-node [this keyseq f args]
    (fn [read]
      (let [layer (at-revision layer (current-revision this))
            canon (id-transform layer read)]
        (-> (apply layer/update-in-node layer
                   (dispatch-update keyseq f args
                                    (fn assoc* [id val]
                                      [[] assoc [(canon id) (update val :edges map-keys canon)]])
                                    (fn dissoc* [id]
                                      [[] dissoc [(canon id)]])
                                    (fn update* [id keys]
                                      (fix-update keyseq f args canon))))
            (update-wrap-read (fn modify-id [read]
                                (fn [layer' keyseq & [not-found]]
                                  (read layer' (if (layer/same? layer' layer)
                                                 (update-keyseq keyseq (id-transform layer' read))
                                                 keyseq)
                                        not-found))))
            (invoke read)))))

  layer/Optimized
  (query-fn [this keyseq not-found f]
    (when (seq keyseq)
      (let [layer (at-revision layer (current-revision this))
            canon (id-transform layer graph/get-in-node)]
        (layer/query-fn layer (update-keyseq keyseq canon) not-found f))))

  layer/Parent
  (children [this]
    (layer/children layer))
  (child [this child-name]
    (when-let [c (layer/child layer child-name)]
      (if (wrap-child? child-name)
        (ForwardLayer. c id-transform wrap-child?)
        c))))

(defn make
  "Wraps a layer so that reads to it have their node ids changed according to a transform function.
   The id-transform function will be called with an id and a revisioned view of the underlying
   layer, and should return the id to read on it."
  [layer id-transform wrap-child?]
  (ForwardLayer. layer id-transform wrap-child?))