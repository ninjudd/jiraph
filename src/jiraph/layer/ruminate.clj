(ns jiraph.layer.ruminate
  (:use jiraph.wrapped-layer
        useful.debug
        [jiraph.utils :only [assert-length]]
        [useful.utils :only [returning adjoin]]
        [useful.map :only [assoc-in*]])
  (:require [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [retro.core :as retro :refer [at-revision current-revision]]))

(defwrapped RuminatingLayer [input-layer output-layers ruminate]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)]
          (ruminate (at-revision input-layer rev)
                    (for [[name layer] output-layers]
                      (at-revision layer rev))
                    keyseq f args))
        (wrap-forwarded-reads this input-layer)))

  Associate
  (associations [this]
    (map first output-layers))
  (associated-layer [this association]
    (first (for [[name layer] output-layers
                 :when (= name association)]
             (at-revision layer (current-revision this)))))

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

;;  will be called once per write, passed a map with keys :keyseq, :old, and :new
(defn make [input outputs write]
  (RuminatingLayer. input (vec outputs) write))

;; TODO needs better name
(defn dispatch-update [keyseq f args assoc-fn dissoc-fn update-fn]
  (if (empty? keyseq)
    (condp = f
      assoc (let [[id value] (assert-length 2 args)]
              (assoc-fn id value))
      dissoc (let [[id] (assert-length 1 args)]
               (dissoc-fn id))
      (throw (IllegalArgumentException.
              (format "Can't perform function %s at top level"
                      f))))
    (update-fn (first keyseq) (rest keyseq))))

;; TODO what to do about edges that have more than just deleted on them? we can't use was-present
(defn changed-edges [old-edges new-edges]
  (reduce (fn [edges [edge-id edge]]
            (let [was-present (not (:deleted edge))
                  is-present  (boolean
                               (when-let [e (get edges edge-id)]
                                 (not (:deleted e))))]
              (if (= was-present is-present)
                (dissoc edges edge-id)
                (assoc-in* edges [edge-id :deleted] was-present))))
          new-edges old-edges))

(defn edges-map [keys val]
  (cond (empty? keys)           (get val :edges {})
        (= :edges (first keys)) (assoc-in* {} (rest keys) val)
        :else                   {}))

(defn incoming [outgoing-layer incoming-layer]
  (make outgoing-layer {:incoming incoming-layer}
        (fn [outgoing [incoming] keyseq f args]
          (let [source-update (apply graph/update-in-node outgoing keyseq f args)]
            (fn [read]
              (let [ ;; TODO is there an easy/useful way to pull out the first two bindings?
                    source-actions (source-update read)
                    read' (graph/advance-reader read source-actions)
                    [read-old read-new] (for [read [read read']]
                                          (fn [id]
                                            (read outgoing-layer [id :edges])))

                    [from-id old-edges new-edges]
                    (dispatch-update keyseq f args
                                     (fn [id val] ;; top-level assoc
                                       [id (read-old id) (:edges val)])
                                     (fn [id] ;; top-level dissoc
                                       [id (read-old id) {}])
                                     (fn [id keys] ;; anything else
                                       (cons id
                                             (if (= f adjoin)
                                               (let [[val] (assert-length 1 args)]
                                                 [{} (edges-map keys val)])
                                               [(read-old id) (read-new id)]))))]
                (into source-actions
                      (for [[to-id edge] (changed-edges old-edges new-edges)
                            ;; TODO accept transform function from outgoing->incoming edge
                            :let [update (graph/update-in-node incoming [to-id :edges from-id]
                                                               adjoin edge)]
                            action (update read')]
                        action))))))))
