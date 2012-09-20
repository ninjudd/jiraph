(ns jiraph.layer.ruminate
  (:use jiraph.wrapped-layer
        useful.debug
        [useful.utils :only [returning adjoin]])
  (:require [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [retro.core :as retro :refer [at-revision current-revision]]
))

(defwrapped RuminatingLayer [input-layer output-layers write]
  layer/Basic
  (update-in-node [this keyseq f args]
    (-> (let [rev (current-revision this)]
          (write (at-revision input-layer rev)
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

;; trigger will be called once per write, passed a map with keys :keyseq, :old, and :new
(defn make [input outputs write]
  (RuminatingLayer. input (vec outputs) write))
