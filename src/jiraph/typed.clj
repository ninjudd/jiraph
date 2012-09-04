(ns jiraph.typed
  (:use [jiraph.core :only [layer]]
        [jiraph.layer :only [Basic Optimized Schema get-node schema update-fn assoc-node]]
        [jiraph.utils :only [meta-keyseq? edges-keyseq deleted-edge-keyseq deleted-node-keyseq]]
        [jiraph.wrapped-layer :only [defwrapped]]
        [retro.core :only [dotxn]]
        [clojure.core.match :only [match]]
        [useful.map :only [map-vals-with-keys update update-in*]]
        [useful.fn :only [fix fixing]]
        [useful.utils :only [adjoin]]
        [useful.experimental :only [prefix-lookup]]
        [useful.datatypes :only [assoc-record]])
  (:require [jiraph.graph :as graph]))

(defn edge-validator [layer id]
  (or ((:type-lookup layer) id)
      (throw (IllegalArgumentException. (format "%s is not a valid node on layer %s"
                                                id (pr-str layer))))))

(defn validate-edges [layer from-id to-ids valid?]
  (when-let [broken-edges (seq (remove valid? to-ids))]
    (throw (IllegalArgumentException.
            (format "%s can't have edges to %s on layer %s"
                    from-id (pr-str broken-edges) (pr-str layer))))))

;; the multimap is for bookkeeping/reference only; the type-lookup function is derived from it at
;; construction time, and is always used instead because it is much faster. type-lookup is a
;; function taking a node-id and returning (if the node's type is valid as a from-edge on this
;; layer) another function. That function takes in a node-id and returns truthy iff it is a valid
;; destination node for an edge from the first node-id.
(defwrapped TypedLayer [layer type-multimap type-lookup]
  Basic
  (assoc-node [this id attrs]
    (validate-edges this id (keys (:edges attrs)) (edge-validator this id))
    (assoc-node layer id attrs))

  Optimized
  (update-fn [this keyseq f]
    (when-let [layer-update-fn (update-fn layer keyseq f)]
      (if (meta-keyseq? keyseq)
        layer-update-fn
        (let [from-id (first keyseq)
              validate-edge (edge-validator this from-id)]
          (if-let [get-edge-ids (match (rest keyseq)
                                  ([] :seq) (comp keys :edges)
                                  ([:edges] :seq) keys
                                  ([:edges to-id & _] :seq) (constantly [to-id]))]
            (if-not (= adjoin f)
              (throw (IllegalArgumentException.
                      (format "Can't guarantee typing of %s on typed layer" f)))
              (fn [arg]
                (validate-edges this from-id (get-edge-ids arg) validate-edge)
                (layer-update-fn arg)))
            layer-update-fn)))))

  Schema
  (schema [this node-id]
    (when (type-lookup node-id)
      (schema layer node-id))))

(defn typed-layer [base-layer types]
  (TypedLayer. base-layer types
               (prefix-lookup (for [[from-type to-types] types]
                                [from-type (prefix-lookup (for [to-type to-types]
                                                            [to-type true]))]))))

(defn without-typing [^TypedLayer typed-layer]
  (.layer typed-layer))
