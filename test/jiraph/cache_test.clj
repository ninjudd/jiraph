(ns jiraph.cache-test
  (:use clojure.test jiraph.core
        [jiraph.walk :only
         [defwalk path paths *parallel-follow* intersection enable-walk-cache! reset-walk-cache!]]
        [jiraph.walk.predicates :only [at-limit]]
        [jiraph.codecs.protobuf :only [protobuf-codec]]
        [fogus.unk :only [memo-lru snapshot]])
  (:require [jiraph.masai-layer :as bal]
            [jiraph.stm-layer :as stm]
            [masai.tokyo :as tokyo])
  (:import [jiraph Test$Node]))

(def test-graph
  {:foo (bal/make (tokyo/make {:path "/tmp/jiraph-cached-walk-test-foo" :create true})
                  :codec-fns {:node (protobuf-codec Test$Node)})})

(defwalk full-walk
  :add?      true
  :traverse? true
  :cache     true)

(deftest test-lru
  (enable-walk-cache! memo-lru 5)
  (with-graph test-graph
    (truncate!)

    (assoc-node! :foo "1" {:edges {"2" {:a "foo"} "3" {:a "bar"}}})
    (assoc-node! :foo "2" {:edges {"5" {:a "foo"} "6" {:a "bar"}}})
    (assoc-node! :foo "4" {:edges {"7" {:a "foo"} "8" {:a "bar"}}})
    (assoc-node! :foo "8" {:edges {"8" {:a "foo"} "9" {:a "bar"}}}))

  (let [foci ["1" "2" "4" "8"]]
    (doseq [focus foci]
      (dotimes [n 2]
        (full-walk focus)))
    (is (= (sort foci)
           (sort (map first (keys (snapshot jiraph.walk/cached-walk))))))
    (reset-walk-cache!)
    (is (= 0 (count(keys (snapshot jiraph.walk/cached-walk)))))))