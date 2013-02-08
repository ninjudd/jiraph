(ns flatland.jiraph.layer.stm-test
  (:use clojure.test flatland.jiraph.graph flatland.jiraph.layer.stm
        [flatland.retro.core :as retro :only [at-revision current-revision]]
        [flatland.useful.map :only [keyed]])
  (:require [flatland.jiraph.layer :as layer]))

;; STMLayer can't optimize anything, so add reference/testing implementations.
(extend-type flatland.jiraph.layer.stm.STMLayer
  flatland.jiraph.layer/Optimized
  (query-fn [this keyseq not-found f]
    (when (= 'specialized-count f)
      (fn [counter]
        (do (swap! counter inc)
            (count (get-in (:nodes (now this)) keyseq not-found))))))
  (update-fn [this [id key :as keyseq] f]
    (when (= :edges key)
      (fn [& args]
        (let [old (get-in (:nodes (now this)) keyseq)
              new (apply f old args)]
          (alter (:store this)
                 assoc-in (list* (current-rev this) :nodes keyseq)
                 new)
          (keyed [old new]))))))

(deftest persist-test
  (let [filename "./stm.layer"
        master (make filename)]
    (txn (assoc-node master "test" {:name "mindy"}))
    (layer/close master)
    (is (= {:name "mindy"}
           (get-node (doto (make filename) layer/open)
                     "test")))
    (-> filename java.io.File. .delete)))

(deftest queryfn-test
  (let [master (make)
        counter (atom 0)]
    (txn (assoc-node master "mikey" {:height {:feet 5 :inches 4}
                                     :edges {"jennifer" {:rel :child}}}))
    (is (= 2 (query-in-node master ["mikey" :height] 'specialized-count counter)))
    (is (= 1 @counter))))
