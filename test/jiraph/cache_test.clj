(ns jiraph.cache-test
  (:use clojure.test jiraph.graph
        [jiraph.walk :only [defwalk path paths *parallel-follow* intersection]]
        [jiraph.walk.predicates :only [at-limit]]
        [fogus.unk :only [snapshot]])
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
  (with-graph test-graph
    (truncate!)

    (is (= ["1"] (:ids (full-walk "1"))))

    (add-node! :foo "1" {:edges {"2" {:a "foo"} "3" {:a "bar"}}})
    (add-node! :foo "2" {:edges {"5" {:a "foo"} "6" {:a "bar"}}})
    (add-node! :foo "4" {:edges {"7" {:a "foo"} "8" {:a "bar"}}})
    (add-node! :foo "8" {:edges {"8" {:a "foo"} "9" {:a "bar"}}}))

  (full-walk "1")
  (full-walk "2")
  (full-walk "4")
  (full-walk "8")
  (prn (snapshot jiraph.walk/cached-walk)))