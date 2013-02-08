(ns flatland.jiraph.ruminate
  (:use flatland.jiraph.wrapped-layer
        flatland.useful.debug
        [flatland.useful.utils :only [returning adjoin]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.map :only [assoc-in*]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update]]
            [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

;;; TODO make sure wrapping layers like merge and ruminate use correctly-revisioned versions of
;;; their child layers (eg ruminate's outputs, and the id layer for merges). Add tests verifying
;;; that this works, as well.

(defwrapped RuminatingLayer [input-layer output-layers ruminate]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)
              revisioned-layers (cons (at-revision input-layer rev)
                                      (for [[name layer] output-layers]
                                        (at-revision layer rev)))
              ioval (ruminate (first revisioned-layers) ;; input layer
                              (rest revisioned-layers)  ;; output layers
                              keyseq f args)]
          (fn [read]
            (let [actions (ioval read)
                  written-layers (set (map :layer actions))]
              (into actions
                    (for [layer revisioned-layers
                          :when (not (contains? written-layers layer))]
                      {:layer layer, :write (constantly nil), :wrap-read identity})))))
        (update-wrap-read forward-reads this input-layer)))

  layer/Parent
  (children [this]
    (reduce into #{}
            [(map first output-layers)
             (layer/children input-layer)]))
  (child [this child-name]
    (or (first (for [[name layer] output-layers
                     :when (= name child-name)]
                 (at-revision layer (current-revision this))))
        (layer/child (at-revision input-layer (current-revision this)) child-name)))

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
    (retro/txn-rollback! input-layer))

  retro/OrderedRevisions
  (max-revision [this]
    (apply min (or (seq (remove #{Double/POSITIVE_INFINITY}
                                (map retro/max-revision
                                     (cons input-layer (map second output-layers)))))
                   [0])))
  (touch [this]
    (doseq [layer (cons input-layer (map second output-layers))]
      (retro/touch (at-revision layer (current-revision this))))))

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

(defn incoming
  "Wrap outgoing-layer with a ruminating layer that stores incoming edges on incoming-layer. If
  provided, outgoing->incoming is called on each outgoing edge's data to decide what data to write
  on the corresponding incoming edge."
  ([outgoing-layer incoming-layer]
     (incoming outgoing-layer incoming-layer (fn [edge]
                                               (not-empty (select-keys edge [:exists])))))
  ([outgoing-layer incoming-layer outgoing->incoming]
     (make outgoing-layer [[:incoming incoming-layer]]
           (fn [outgoing [incoming] keyseq f args]
             (let [source-update (apply update-in-node outgoing keyseq f args)]
               (fn [read]
                 (let [source-actions (source-update read)
                       read' (graph/advance-reader read source-actions)
                       [read-old read-new] (for [read [read read']]
                                             (fn [id]
                                               (read outgoing [id :edges])))]
                   (->> (if (and (seq keyseq) (= f adjoin))
                          (let [[from-id & keys] keyseq]
                            (for [[to-id edge] (apply edges-map keys (assert-length 1 args))
                                  :let [incoming-edge (outgoing->incoming edge)]
                                  :when incoming-edge]
                              ((update-in-node incoming [to-id :edges from-id]
                                               adjoin incoming-edge)
                               read')))
                          (let [[from-id new-edges] (dispatch-update keyseq f args
                                                                     (fn [id val] ;; top-level assoc
                                                                       [id (:edges val)])
                                                                     (fn [id] ;; top-level dissoc
                                                                       [id {}])
                                                                     (fn [id keys] ;; anything else
                                                                       [id (read-new id)]))
                                old-edges (read-old from-id)]
                            (concat (for [[to-id edge] new-edges
                                          :let [incoming-edge (outgoing->incoming edge)]
                                          :when incoming-edge]
                                      ((update-in-node incoming [to-id :edges from-id]
                                                       (constantly incoming-edge))
                                       read'))
                                    (for [to-id (keys old-edges)
                                          :when (not (contains? new-edges to-id))]
                                      ((update-in-node incoming [to-id :edges]
                                                       dissoc from-id)
                                       read')))))
                        (apply concat)
                        (into source-actions)))))))))

(defn top-level-indexer [source index field index-fieldname]
  (make source [[field index]]
        (fn [source [index] keyseq f args]
          (fn [read]
            (let [source-update ((apply update-in-node source keyseq f args) read)
                  read' (graph/advance-reader read source-update)]
              (into source-update
                    (when-let [id (first (if (seq keyseq)
                                           (when (or (not (next keyseq))
                                                     (= field (second keyseq)))
                                             keyseq)
                                           args))]
                      (let [[old-idx new-idx] ((juxt read read') source [id field])]
                        (when (not= old-idx new-idx)
                          (letfn [(record [idx exists]
                                    (when idx
                                      ((update-in-node index [idx index-fieldname]
                                                       adjoin {id exists}) read)))]
                            (concat (record new-idx true)
                                    (record old-idx false))))))))))))

(defn changelog [source dest]
  (make source [[:changelog dest]]
        (fn [source [dest] keyseq f args]
          (graph/compose (apply update-in-node source keyseq f args)
                         (update-in-node dest [(str "revision-" (inc (current-revision dest)))
                                               :ids]
                                         adjoin [(dispatch-update keyseq f args
                                                                  (fn assoc* [id value]
                                                                    id)
                                                                  (fn dissoc* [id]
                                                                    id)
                                                                  (fn update* [id keys]
                                                                    id))])))))

;; - eventually, switch from deleted to exists, but not yet
;; - until then, copy all data to incoming edges, whether using adjoin or not
