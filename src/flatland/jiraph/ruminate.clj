(ns flatland.jiraph.ruminate
  (:use flatland.jiraph.wrapped-layer
        flatland.useful.debug
        [flatland.useful.utils :only [returning adjoin]]
        [flatland.useful.map :only [map-vals]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.map :only [assoc-in*]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update child]]
            [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

;;; TODO make sure wrapping layers like merge and ruminate use correctly-revisioned versions of
;;; their child layers (eg ruminate's outputs, and the id layer for merges). Add tests verifying
;;; that this works, as well.

(defwrapped RuminatingLayer
  [primary-layer secondary-layers ruminate]
  [primary-layer (cons primary-layer (map second secondary-layers))]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)
              revisioned-layers (cons (at-revision primary-layer rev)
                                      (for [[name layer] secondary-layers]
                                        (at-revision layer rev)))
              ioval (ruminate (first revisioned-layers) ;; primary layer
                              (rest revisioned-layers)  ;; secondary layers
                              keyseq f args)]
          (fn [read]
            (let [actions (ioval read)
                  written-layers (set (map :layer actions))]
              (into actions
                    (for [layer revisioned-layers
                          :when (not (contains? written-layers layer))]
                      {:layer layer, :write (constantly nil), :wrap-read identity})))))
        (update-wrap-read forward-reads this primary-layer)))

  layer/Parent
  (children [this]
    (reduce into #{}
            [(map first secondary-layers)
             (layer/children primary-layer)]))
  (child [this child-name]
    (or (first (for [[name layer] secondary-layers
                     :when (= name child-name)]
                 (at-revision layer (current-revision this))))
        (layer/child (at-revision primary-layer (current-revision this)) child-name)))

  retro/OrderedRevisions
  (touch [this]
    (doseq [layer (cons primary-layer (map second secondary-layers))]
      (retro/touch (at-revision layer (current-revision this))))))

;; write will be called once per update, passed args like: (write primary-layer [output1 output2...]
;; keyseq f args) It should return a jiraph io-value (a function of read; see update-in-node's
;; contract)
(defn make [primary-layer secondary-layers write]
  (RuminatingLayer. primary-layer (vec secondary-layers) write))

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
     (incoming outgoing-layer incoming-layer #(select-keys % [:exists])))
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
                                  :when (seq incoming-edge)]
                              ((update-in-node incoming [to-id :edges from-id]
                                               adjoin incoming-edge)
                               read')))
                          (let [[from-id new-edges] (dispatch-update keyseq f args
                                                                     (fn assoc* [id val]
                                                                       [id (:edges val)])
                                                                     (fn dissoc* [id]
                                                                       [id {}])
                                                                     (fn update* [id keys]
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
                                               :edges
                                               (dispatch-update keyseq f args
                                                                (fn assoc* [id value]
                                                                  id)
                                                                (fn dissoc* [id]
                                                                  id)
                                                                (fn update* [id keys]
                                                                  id))]
                                         adjoin {:exists true})))))

(defn- ruminate-merge [merge-layer base-layers keyseq f args]
  ;; ...
  )

(defn- ruminate-merging [base-layer [phantom-layer merge-layer] keyseq f args]
  (fn [read]
    (-> (if-let [head (merge-head read merge-layer (get-id keyseq))]
          (let [keyseq (update keyseq for head-id)]
            (apply compose (for [layer [base-layer phantom-layer]]
                             (update-in-node layer keyseq f args))))
          (update-in-node base-layer keyseq f args))
        (invoke read))))

(defn merged
  "layers needs to be a map of layer names to base layers. The base layer will be used to store a
   merged view of tha data written to the merging layer, as determined by merges written to the
   merge-layer. Each base layer must have a child named :phantom, which will be used to store
   internal bookkeeping data, and should not be used by client code.

   Will return a list, [new-merge-layer {layer-name1 merging-layer1
                                         layer-name2 merging-layer2 ...}].

   Writes to these returned layers will automatically update each other as needed to keep the merged
   views consistent."
  [merge-layer layers]
  [(make merge-layer layers ruminate-merge)
   (map-vals layers
             (fn [base]
               (make base [[:phantom (child base :phantom)]
                           [:merge merge-layer]]
                     ruminate-merging)))])


#_(merged m {:tree (parent/make tree-base {:phantom tree-phantom})
             :profile-data (parent/make data-base {:phantom data-phantom})})
