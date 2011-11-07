(ns jiraph.stm-test
  (:use clojure.test jiraph.graph jiraph.stm-layer
        [retro.core :as retro :only [dotxn at-revision current-revision]])
  (:require [jiraph.layer :as layer]))

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
        update-counter (atom 0)]
    (dotxn master
      (-> master
          (assoc-node "mikey" {:height {:feet 5 :inches 4}
                               :edges {"jennifer" {:rel :child}}})))
    (is (= 2 (query-in-node master ["mikey" :height] 'specialized-count update-counter)))
    (is (= 1 @update-counter))))
