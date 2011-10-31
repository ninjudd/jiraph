(ns jiraph.cache-test
  (:use clojure.test jiraph.graph
        [jiraph.walk :only [defwalk path paths *parallel-follow* intersection enable-walk-caching!]]
        [jiraph.walk.predicates :only [at-limit]]
        [fogus.unk :only [memo-lru snapshot]])
  (:require [jiraph.masai-layer :as bal]
            [jiraph.stm-layer :as stm]
            [masai.tokyo :as tokyo]
            [cereal.reader :as raf]
            [cereal.protobuf :as paf])
  (:import [jiraph Test$Node]))

(def test-graph
  {:foo (bal/make (tokyo/make {:path "/tmp/jiraph-cached-walk-test-foo" :create true})
                  (paf/make Test$Node))})

(defwalk full-walk
  :add?      true
  :traverse? true
  :cache     true)

(deftest test-lru
  (enable-walk-caching! memo-lru 5)
  (with-graph test-graph
    (truncate!)

    (add-node! :foo "1" {:edges {"2" {:a "foo"} "3" {:a "bar"}}})
    (add-node! :foo "2" {:edges {"5" {:a "foo"} "6" {:a "bar"}}})
    (add-node! :foo "4" {:edges {"7" {:a "foo"} "8" {:a "bar"}}})
    (add-node! :foo "8" {:edges {"8" {:a "foo"} "9" {:a "bar"}}}))

  (let [foci ["1" "2" "4" "8"]]
    (doseq [focus foci]
      (dotimes [n 2]
        (full-walk focus)))
    (is (= (sort foci)
           (sort (map first (keys (snapshot jiraph.walk/cached-walk))))))))