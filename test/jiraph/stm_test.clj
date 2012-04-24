(ns jiraph.stm-test
  (:use clojure.test jiraph.graph jiraph.stm-layer
        [retro.core :as retro :only [dotxn at-revision current-revision]]
        [useful.map :only [keyed]])
  (:require [jiraph.layer :as layer]))

;; STMLayer can't optimize anything, so add reference/testing implementations.
(extend-type jiraph.stm_layer.STMLayer
  jiraph.layer/Optimized
  (query-fn [this keyseq f]
    (when (= 'specialized-count f)
      (fn [counter]
        (do (swap! counter inc)
            (count (get-in (:nodes (now this)) keyseq))))))
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
    (dotxn master
      (-> master
          (assoc-node "test" {:name "mindy"})))
    (layer/close master)
    (is (= {:name "mindy"}
           (get-node (doto (make filename) layer/open)
                     "test")))
    (-> filename java.io.File. .delete)))

(deftest queryfn-test
  (let [master (make)
        counter (atom 0)]
    (dotxn master
      (-> master
          (assoc-node "mikey" {:height {:feet 5 :inches 4}
                               :edges {"jennifer" {:rel :child}}})))
    (is (= 2 (query-in-node master ["mikey" :height] 'specialized-count counter)))
    (is (= 1 @counter))))
