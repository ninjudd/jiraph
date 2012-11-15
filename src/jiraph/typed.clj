(ns jiraph.typed
  (:use [jiraph.core :only [layer]]
        useful.debug
        [jiraph.layer :only [Basic Optimized Schema get-node schema update-in-node query-fn]]
        [jiraph.utils :only [edges-keyseq deleted-edge-keyseq deleted-node-keyseq]]
        [jiraph.wrapped-layer :only [defwrapped update-wrap-read forward-reads]]
        [clojure.core.match :only [match]]
        [useful.map :only [map-vals-with-keys update update-in*]]
        [useful.fn :only [fix fixing]]
        [useful.utils :only [adjoin]]
        [useful.seq :only [assert-length]]
        [useful.experimental :only [prefix-lookup]]
        [useful.datatypes :only [assoc-record]])
  (:require [jiraph.layer :as layer]))

(defn edge-validator [layer id]
  (or ((:type-lookup layer) id)
      (throw (IllegalArgumentException. (format "%s is not a valid node on layer %s"
                                                id (pr-str layer))))))

(defn validate-edges [layer from-id to-ids]
  (let [valid? (edge-validator layer from-id)]
    (when-let [broken-edges (seq (remove valid? to-ids))]
      (throw (IllegalArgumentException.
              (format "%s can't have edges to %s on layer %s"
                      from-id (pr-str broken-edges) (pr-str layer)))))))

;; TODO can't figure out how to do this with core.match
(defn writeable-area?* [layer keyseq]
  (let [lookup (:type-lookup layer)]
    (match keyseq
      ([id :edges to-id & more] :seq) (when-let [edge-types (lookup id)]
                                        (edge-types to-id))
      ([id & more] :seq) (lookup id)
      ([] :seq) true)))

(defn writeable-area? [layer keyseq]
  (or (empty? keyseq)
      (let [lookup (:type-lookup layer)
            edge-checker (lookup (first keyseq))]
        (and edge-checker
             (let [keys (next keyseq)]
               (or (not keys) ;; keyseq is [id]
                   (not= :edges (first keys)) ;; something not under edges
                   (let [edge-path (next keys)]
                     (or (not edge-path) ;; just [id :edges]
                         (edge-checker (first edge-path))))))))))

;; the multimap is for bookkeeping/reference only; the type-lookup function is derived from it at
;; construction time, and is always used instead because it is much faster. type-lookup is a
;; function taking a node-id and returning (if the node's type is valid as a from-edge on this
;; layer) another function. That function takes in a node-id and returns truthy iff it is a valid
;; destination node for an edge from the first node-id.
(defwrapped TypedLayer [layer type-multimap type-lookup]
  Basic
  (get-node [this id not-found]
    (if (writeable-area? this [id])
      (get-node layer id not-found)
      not-found))
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
            (update-wrap-read forward-reads this layer))))

  Optimized
  (query-fn [this keyseq not-found f]
    (if (writeable-area? this keyseq)
      (query-fn layer keyseq not-found f)
      (fn [& args]
        (apply f not-found args))))

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
