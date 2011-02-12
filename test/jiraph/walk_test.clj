(ns jiraph.walk-test
  (:use clojure.test jiraph.graph
        [jiraph.walk :only [defwalk path paths limit]])
  (:require [jiraph.byte-append-layer :as bal]
            [jiraph.tokyo-database :as tokyo]
            [jiraph.reader-append-format :as raf]
            [jiraph.protobuf-append-format :as paf])
  (:import [jiraph Test$Node]))

(def test-graph
  {:foo (bal/make (tokyo/make {:path "/tmp/jiraph-walk-test-foo" :create true}) (raf/make))
   :bar (bal/make (tokyo/make {:path "/tmp/jiraph-walk-test-bar" :create true}) (paf/make Test$Node))
   :baz (bal/make (tokyo/make {:path "/tmp/jiraph-walk-test-baz" :create true}) (raf/make))})

(defwalk full-walk
  :add?      true
  :traverse? true)

(deftest simple-walk
  (with-graph test-graph
    (truncate!)

    (is (= ["1"] (:ids (full-walk "1"))))

    (add-node! :foo "1" {:edges {"2" {:a "foo"} "3" {:a "bar"}}})
    (add-node! :bar "2" {:edges {"3" {:a "foo"} "4" {:a "bar"}}})
    (add-node! :baz "2" {:edges {"5" {:a "foo"} "6" {:a "bar"}}})
    (add-node! :baz "4" {:edges {"7" {:a "foo"} "8" {:a "bar"}}})
    (add-node! :baz "8" {:edges {"1" {:a "foo"} "2" {:a "bar"}}})

    (let [walk (full-walk "1")]
      (is (= 8 (:result-count walk)))
      (is (= ["1" "2" "3" "4" "5" "6" "7" "8"] (:ids walk)))
      (is (= ["1" "2" "4" "8"] (map :id (path walk "8"))))
      (is (= ["1" "2" "4" "7"] (map :id (path walk "7"))))
      (is (= ["1" "2" "6"]     (map :id (path walk "6"))))
      (is (= ["1" "2"]         (map :id (path walk "2"))))
      (is (= [["1" "2"] ["1" "2" "4" "8" "2"]] (map (partial map :id) (paths walk "2"))))
      (is (= [["1"]     ["1" "2" "4" "8" "1"]] (map (partial map :id) (paths walk "1")))))

    (testing "early termination"
      (let [walk (full-walk "1" :terminate? (limit 5))]
        (is (= 5 (:result-count walk)))
        (is (= ["1" "2" "3" "4" "5"] (:ids walk)))
        (is (= nil (path walk "8")))
        (is (= nil (path walk "7")))
        (is (= nil (path walk "6")))
        (is (= ["1" "2"]   (map :id (path walk "2"))))
        (is (= [["1" "2"]] (map (partial map :id) (paths walk "2"))))
        (is (= [["1"]]     (map (partial map :id) (paths walk "1"))))))))

