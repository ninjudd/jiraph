(ns jiraph.layer.ruminate
  (:use jiraph.wrapped-layer
        useful.debug
        [useful.utils :only [returning adjoin]])
  (:require [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [retro.core :as retro]))

(defwrapped RuminatingLayer [input-layer output-layers trigger]
  layer/Basic
  (assoc-node! [this id attrs]
    (let [old (graph/get-node input-layer id)]
      (trigger (graph/assoc-node! input-layer id attrs))))
  (dissoc-node! [this id]
    (let [old (graph/get-node input-layer id)]
      (trigger (graph/dissoc-node! input-layer id))))

  layer/Optimized
  (update-fn [this keyseq f]
    (fn [& args]
      (trigger (apply graph/update-in-node! input-layer keyseq f args))))

  layer/Preferences
  (manage-incoming? [this] false)
  (manage-changelog? [this] false)
  (single-edge? [this] false)

  retro/Transactional
  (txn-begin! [this]
    (returning (retro/txn-begin! input-layer)
      (doseq [layer output-layers]
        (retro/txn-begin! layer))))
  (txn-commit! [this]
    (doseq [layer (rseq output-layers)]
      (retro/txn-commit! layer))
    (retro/txn-commit! input-layer))
  (txn-rollback! [this]
    (doseq [layer (rseq output-layers)]
      (retro/txn-rollback! layer))
    (retro/txn-rollback! input-layer)))

;; trigger will be called once per write, passed a map with keys :keyseq, :old, and :new
(defn make [input outputs trigger]
  (RuminatingLayer. input (vec outputs)
                    (fn [change]
                      (doto change (trigger)))))
