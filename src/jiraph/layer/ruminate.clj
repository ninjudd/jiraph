(ns jiraph.layer.ruminate
  (:use jiraph.wrapped-layer
        useful.debug
        [jiraph.utils :only [assert-length]]
        [useful.utils :only [returning adjoin]]
        [useful.map :only [assoc-in*]])
  (:require [jiraph.layer :as layer :refer [dispatch-update]]
            [jiraph.graph :as graph :refer [update-in-node]]
            [retro.core :as retro :refer [at-revision current-revision]]))

;;; TODO make sure wrapping layers like merge and ruminate use correctly-revisioned versions of
;;; their child layers (eg ruminate's outputs, and the id layer for merges). Add tests verifying
;;; that this works, as well.

(defwrapped RuminatingLayer [input-layer output-layers ruminate]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)]
          (ruminate (at-revision input-layer rev)
                    (for [[name layer] output-layers]
                      (at-revision layer rev))
                    keyseq f args))
        (update-wrap-read forward-reads this input-layer)))

  Parent
  (children [this]
    (map first output-layers))
  (child [this child-name]
    (first (for [[name layer] output-layers
                 :when (= name child-name)]
             (at-revision layer (current-revision this)))))

  layer/Layer
  (open [this]
    (doseq [layer (cons input-layer (map second output-layers))]
      (layer/open layer)))
  (close [this]
    (doseq [layer (cons input-layer (map second output-layers))]
      (layer/close layer)))
  (truncate! [this]
    (doseq [layer (cons input-layer (map second output-layers))]
      (layer/truncate! layer)))

  retro/Transactional
  (txn-begin! [this]
    (returning (retro/txn-begin! input-layer)
      (doseq [[name layer] output-layers]
        (retro/txn-begin! layer))))
  (txn-commit! [this]
    (doseq [[name layer] (rseq output-layers)]
      (retro/txn-commit! layer))
    (retro/txn-commit! input-layer))
  (txn-rollback! [this]
    (doseq [[name layer] (rseq output-layers)]
      (retro/txn-rollback! layer))
    (retro/txn-rollback! input-layer)))

;; write will be called once per update, passed args like: (write input-layer [output1 output2...]
;; keyseq f args) It should return a jiraph io-value (a function of read; see update-in-node's
;; contract)
(defn make [input outputs write]
  (RuminatingLayer. input (vec outputs) write))

(defn edges-map
  "Given a keyseq (not including a node-id, and possibly empty) and a value at that keyseq,
   returns the the :edges attribute of the value, or {} if the keyseq does not match :edges."
  [keys val]
  (cond (empty? keys)           (get val :edges {})
        (= :edges (first keys)) (assoc-in* {} (rest keys) val)
        :else                   {}))

;; TODO accept transform function from outgoing->incoming edge so we can store other data.
(defn incoming
  "Wrap outgoing-layer with a ruminating layer that stores incoming edges on
incoming-layer. Currently does not support storing any data on incoming edges other than :deleted
true/false."
  [outgoing-layer incoming-layer]
  (make outgoing-layer [[:incoming incoming-layer]]
        (fn [outgoing [incoming] keyseq f args]
          (let [source-update (apply update-in-node outgoing keyseq f args)]
            (fn [read]
              (let [source-actions (source-update read)
                    read' (graph/advance-reader read source-actions)
                    [read-old read-new] (for [read [read read']]
                                          (fn [id]
                                            (read outgoing-layer [id :edges])))]
                (->> (if (and (seq keyseq) (= f adjoin))
                       (let [[from-id & keys] keyseq]
                         (for [[to-id edge] (apply edges-map keys (assert-length 1 args))]
                           ((update-in-node incoming [to-id :edges from-id]
                                            adjoin edge)
                            read')))
                       (let [[from-id new-edges] (dispatch-update keyseq f args
                                                                  (fn [id val] ;; top-level assoc
                                                                    [id (:edges val)])
                                                                  (fn [id] ;; top-level dissoc
                                                                    [id {}])
                                                                  (fn [id keys] ;; anything else
                                                                    [id (read-new id)]))
                             old-edges (read-old from-id)]
                         (concat (for [[to-id edge] new-edges]
                                   ((update-in-node incoming [to-id :edges from-id]
                                                    (constantly edge))
                                    read'))
                                 (for [to-id (keys old-edges)
                                       :when (not (contains? new-edges to-id))]
                                   ((update-in-node incoming [to-id :edges]
                                                    dissoc from-id)
                                    read')))))
                     (apply concat)
                     (into source-actions))))))))

(defn top-level-indexer [source index field]
  (make source [[field index]]
        (fn [source [index] keyseq f args]
          (fn [read]
            (let [source-update ((apply update-in-node source keyseq f args) read)
                  read' (graph/advance-reader read source-update)]
              (reduce into source-update
                      (when-let [id (first (if (seq keyseq)
                                             (when (or (not (next keyseq))
                                                       (= field (second keyseq)))
                                               keyseq)
                                             args))]
                        (let [[old-idx new-idx] ((juxt read read') source [id field])]
                          (when (not= old-idx new-idx)
                            [((update-in-node index [old-idx] disj id) read)
                             ((update-in-node index [new-idx] conj id) read)])))))))))

;; - eventually, switch from deleted to exists, but not yet
;; - until then, copy all data to incoming edges, whether using adjoin or not
