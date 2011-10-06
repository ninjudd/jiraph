(ns jiraph.walk-test
  (:use clojure.test jiraph.graph
        [jiraph.walk :only [defwalk path paths *parallel-follow* intersection]]
        [jiraph.walk.predicates :only [at-limit]])
  (:require [jiraph.masai-layer :as bal]
            [jiraph.stm-layer :as stm]
            [masai.tokyo :as tokyo]
            [cereal.reader :as raf]
            [cereal.protobuf :as paf])
  (:import [jiraph Test$Node]))

(def test-graph
  {:foo (bal/make :db (tokyo/make {:path "/tmp/jiraph-walk-test-foo" :create true}) :format (raf/make))
   :bar (bal/make :db (tokyo/make {:path "/tmp/jiraph-walk-test-bar" :create true}) :format (paf/make Test$Node))
   :baz (bal/make :db (tokyo/make {:path "/tmp/jiraph-walk-test-baz" :create true}) :format (raf/make))
   :stm (stm/make {:single-edge true})})

(defwalk full-walk
  :add?      true
  :traverse? true)

(deftest simple-walk
  (doseq [parallel? [true false]]
    (binding [*parallel-follow* parallel?]
      (with-graph test-graph
        (truncate!)

        (is (= ["1"] (:ids (full-walk "1"))))

        (add-node! :foo "1" {:edges {"2" {:a "foo"} "3" {:a "bar"}}})
        (add-node! :bar "2" {:edges {"3" {:a "foo"} "4" {:a "bar"}}})
        (add-node! :baz "2" {:edges {"5" {:a "foo"} "6" {:a "bar"}}})
        (add-node! :baz "4" {:edges {"7" {:a "foo"} "8" {:a "bar"}}})
        (add-node! :baz "8" {:edges {"8" {:a "foo"} "9" {:a "bar"}}})
        (add-node! :stm "9" {:edge  {:id "2"}})

        (let [walk (full-walk "1")]
          (is (= 9 (:result-count walk)))
          (is (= ["1" "2" "3" "4" "5" "6" "7" "8" "9"] (:ids walk)))
          (is (= ["1" "2" "4" "8"] (map :id (path walk "8"))))
          (is (= ["1" "2" "4" "7"] (map :id (path walk "7"))))
          (is (= ["1" "2" "6"]     (map :id (path walk "6"))))
          (is (= ["1" "2"]         (map :id (path walk "2"))))
          (is (= [["1" "2"] ["1" "2" "4" "8" "9" "2"]] (map (partial map :id) (paths walk "2"))))
          (is (= [["1" "3"]             ["1" "2" "3"]] (map (partial map :id) (paths walk "3")))))

        (testing "early termination"
          (let [walk (full-walk "1" :terminate? (at-limit 5))]
            (is (= 5 (:result-count walk)))
            (is (= ["1" "2" "3" "4" "5"] (:ids walk)))
            (is (= nil (path walk "8")))
            (is (= nil (path walk "7")))
            (is (= nil (path walk "6")))
            (is (= ["1" "2"]   (map :id (path walk "2"))))
            (is (= [["1" "2"]] (map (partial map :id) (paths walk "2"))))
            (is (= [["1"]]     (map (partial map :id) (paths walk "1"))))))

        (testing "intersection"
          (is (= nil
                 (intersection (full-walk "1" :terminate? (at-limit 2))
                               (full-walk "8" :terminate? (at-limit 2)))))
          (is (= '("2")
                 (intersection (full-walk "1" :terminate? (at-limit 3))
                               (full-walk "8" :terminate? (at-limit 3)))))
          (is (= '("2" "3")
                 (intersection (full-walk "1" :terminate? (at-limit 4))
                               (full-walk "8" :terminate? (at-limit 4))))))

        (testing "max-rev"
          (at-revision 33
            (append-node! :foo "1" {:edges {"8" {:a "one"}}}))
          (let [walk (full-walk "1")]
            (is (= 9 (:result-count walk)))
            (is (= ["1" "2" "3" "8" "4" "5" "6" "9" "7"] (:ids walk)))
            (is (= ["1" "8"] (map :id (path walk "8"))))
            (is (= 33  (:max-rev walk)))))))))
