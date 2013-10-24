(ns flatland.jiraph.ruminate
  (:use flatland.jiraph.wrapped-layer
        flatland.useful.debug
        [flatland.useful.utils :only [returning adjoin]]
        [flatland.useful.fn :only [applied]]
        [flatland.useful.map :only [map-vals]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.map :only [assoc-in* update map-keys]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update child]]
            [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.jiraph.parent :as parent]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

;;; TODO make sure wrapping layers like merge and ruminate use correctly-revisioned versions of
;;; their child layers (eg ruminate's outputs, and the id layer for merges). Add tests verifying
;;; that this works, as well.

(defwrapped RuminatingLayer
  [primary-layer secondary-layers ruminate]
  [primary-layer nil secondary-layers]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)
              revisioned-layers (for [layer (cons primary-layer secondary-layers)]
                                  (at-revision layer rev))]
          (ruminate (first revisioned-layers) ;; primary layer
                    (rest revisioned-layers)  ;; secondary layers
                    keyseq f args))
        (update-wrap-read forward-reads this primary-layer))))

;; write will be called once per update, passed args like: (write primary-layer [output1 output2...]
;; keyseq f args) It should return a jiraph io-value (a function of read; see update-in-node's
;; contract)
(defn make [primary-layer secondary-layers write]
  (RuminatingLayer. primary-layer secondary-layers write))

(defn edges-map
  "Given a keyseq (not including a node-id, and possibly empty) and a value at that keyseq,
   returns the the :edges attribute of the value, or {} if the keyseq does not match :edges."
  [keys val]
  (cond (empty? keys)           (get val :edges {})
        (= :edges (first keys)) (assoc-in* {} (rest keys) val)
        :else                   {}))

(defn update-edge-ids [keys val f & args]
  (if (or (empty? keys)
          (= keys [:edges]))
    (-> (assoc-in* {} keys val)
        (update :edges map-keys (applied f) args)
        (get-in keys))
    val))

(defn incoming
  "Wrap outgoing-layer with a ruminating layer that stores incoming edges on incoming-layer. If
  provided, outgoing->incoming is called on each outgoing edge's data to decide what data to write
  on the corresponding incoming edge."
  ([outgoing-layer incoming-layer]
     (incoming outgoing-layer incoming-layer #(select-keys % [:exists])))
  ([outgoing-layer incoming-layer outgoing->incoming]
     (letfn [(ruminate-incoming [outgoing [incoming] keyseq f args]
               (let [source-update (apply update-in-node outgoing keyseq f args)]
                 (fn [read]
                   (let [source-actions (source-update read)
                         read' (graph/advance-reader read source-actions)]
                     (->> (if (and (seq keyseq) (= f adjoin))
                            (let [[from-id & keys] keyseq]
                              (for [[to-id edge] (apply edges-map keys (assert-length 1 args))
                                    :let [incoming-edge (outgoing->incoming edge)]
                                    :when (seq incoming-edge)]
                                ((update-in-node incoming [to-id :edges from-id]
                                                 adjoin incoming-edge)
                                 read')))
                            (let [[read-old read-new] (for [read [read read']]
                                                        (fn [id]
                                                          (read outgoing [id :edges])))
                                  [from-id new-edges]
                                  ,,(dispatch-update keyseq f args
                                                     (fn assoc*  [id val]  [id (:edges val)])
                                                     (fn dissoc* [id]      [id {}])
                                                     (fn update* [id keys] [id (read-new id)]))
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
                          (into source-actions))))))]
       (-> (make outgoing-layer [incoming-layer] ruminate-incoming)
           (parent/make {:incoming incoming-layer})))))

(defn top-level-indexer [source index field index-fieldname]
  (letfn [(ruminate-index [source [index] keyseq f args]
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
                                      (record old-idx false))))))))))]
    (-> (make source [index] ruminate-index)
        (parent/make {field index}))))

(defn changelog
  ([source dest]
     (changelog source dest (partial str "revision-")))
  ([source dest make-revision-id]
     (letfn [(ruminate-changelog [source [dest] keyseq f args]
               (graph/compose (apply update-in-node source keyseq f args)
                              (update-in-node dest [(make-revision-id (inc (current-revision dest)))
                                                    :edges
                                                    (dispatch-update keyseq f args
                                                                     (fn assoc* [id value]
                                                                       id)
                                                                     (fn dissoc* [id]
                                                                       id)
                                                                     (fn update* [id keys]
                                                                       id))]
                                              adjoin {:exists true})))]
       (-> (make source [dest] ruminate-changelog)
           (parent/make {:changelog dest})))))
