(ns jiraph.typed
  (:use [jiraph.core :only [layer]]
        [jiraph.layer :only [Basic Optimized Schema get-node schema update-in-node]]
        [jiraph.utils :only [assert-length edges-keyseq deleted-edge-keyseq deleted-node-keyseq]]
        [jiraph.wrapped-layer :only [defwrapped wrap-forwarded-reads]]
        [retro.core :only [dotxn]]
        [clojure.core.match :only [match]]
        [useful.map :only [map-vals-with-keys update update-in*]]
        [useful.fn :only [fix fixing]]
        [useful.utils :only [adjoin]]
        [useful.experimental :only [prefix-lookup]]
        [useful.datatypes :only [assoc-record]])
  (:require [jiraph.layer :as layer]))

(defn edge-validator [layer id]
  (or ((:type-lookup layer) id)
      (throw (IllegalArgumentException. (format "%s is not a valid node on layer %s"
                                                id (pr-str layer))))))

(defn validate-edges [layer from-id to-ids valid?]
  (let [valid? (edge-validator layer from-id)]
    (when-let [broken-edges (seq (remove valid? to-ids))]
      (throw (IllegalArgumentException.
              (format "%s can't have edges to %s on layer %s"
                      from-id (pr-str broken-edges) (pr-str layer)))))))

;; the multimap is for bookkeeping/reference only; the type-lookup function is derived from it at
;; construction time, and is always used instead because it is much faster. type-lookup is a
;; function taking a node-id and returning (if the node's type is valid as a from-edge on this
;; layer) another function. That function takes in a node-id and returns truthy iff it is a valid
;; destination node for an edge from the first node-id.
(defwrapped TypedLayer [layer type-multimap type-lookup]
  Basic
  (update-in-node [this keyseq f args]
    (do (if (empty? keyseq)
          (condp = f
            dissoc nil
            assoc (let [[id attrs] (assert-length 2 args)]
                    (validate-edges this id (keys (:edges attrs))))
            (throw (IllegalArgumentException. (format "Can't apply function %s at top level"
                                                      f))))
          (if-not (#{assoc adjoin} f)
            (throw (IllegalArgumentException.
                    (format "Can't guarantee typing of %s on typed layer" f)))
            (let [from-id (first keyseq)
                  [attrs] (assert-length 1 args)]
              (validate-edges this from-id
                              (match (rest keyseq)
                                ([] :seq) (keys (:edges attrs))
                                ([:edges] :seq) (keys attrs)
                                ([:edges to-id & _] :seq) [to-id])))))
        (-> (update-in-node layer keyseq f args)
            (wrap-forwarded-reads this layer))))

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
