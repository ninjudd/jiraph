(ns jiraph.merge-test
  (:use clojure.test jiraph.core jiraph.merge)
  (:require [jiraph.masai-layer :as masai]
            [jiraph.layer.ruminate :as ruminate]
            [useful.utils :refer [adjoin]]))

(defn empty-graph [f]
  (let [[id-base id-incoming people-base people-incoming] (repeatedly masai/make-temp)
        id-with-incoming (ruminate/incoming id-base id-incoming)
        people-with-incoming (ruminate/incoming people-base people-incoming)]
    (with-graph {:id     id-with-incoming
                 :people (mergeable-layer people-with-incoming id-with-incoming)}
      (f))))

(use-fixtures :each empty-graph)

(deftest merging
  (testing "nothing is merged initially"
    (is (empty? (merged-into "A")))
    (is (empty? (merged-into "B")))
    (is (= ["A"] (merge-ids "A")))
    (is (= ["B"] (merge-ids "B")))
    (is (= nil (merge-head "A")))
    (is (= nil (merge-head "B")))
    (is (= nil (merge-position "A")))
    (is (= nil (merge-position "B"))))

  (testing "merge two nodes"
    (at-revision 1 (merge-node! "A" "B"))
    (is (= #{"B"}    (merged-into "A")))
    (is (= ["A" "B"] (merge-ids "A")))
    (is (= ["A" "B"] (merge-ids "B")))
    (is (= "A" (merge-head "A")))
    (is (= "A" (merge-head "B")))
    (is (= 0 (merge-position "A")))
    (is (= 1 (merge-position "B"))))

  (testing "cannot re-merge tail"
    (is (thrown-with-msg? Exception #"already merged"
          (merge-node! "C" "B"))))

  (testing "cannot merge into non-head"
    (is (thrown-with-msg? Exception #"already merged"
          (merge-node! "B" "C"))))

  (testing "merge multiple nodes into a single head"
    (at-revision 2 (merge-node! "A" "C"))
    (at-revision 3 (merge-node! "A" "D"))
    (is (= #{"B" "C" "D"} (merged-into "A")))
    (is (= "A" (merge-head "C")))
    (is (= "A" (merge-head "D")))
    (is (= 0 (merge-position "A")))
    (is (= 1 (merge-position "B")))
    (is (= 2 (merge-position "C")))
    (is (= 3 (merge-position "D"))))

  (testing "can view previous merge data with at-revision"
    (at-revision 1
      (is (= #{"B"} (merged-into "A")))
      (is (= nil (merge-head "C")))
      (is (= nil (merge-head "D")))))

  (testing "merge two chains together"
    (at-revision 4 (merge-node! "E" "F"))
    (at-revision 5 (merge-node! "E" "G"))
    (is (= #{"F" "G"} (merged-into "E")))
    (is (= 0 (merge-position "E")))
    (is (= 1 (merge-position "F")))
    (is (= 2 (merge-position "G")))
    (at-revision 6 (merge-node! "A" "E"))
    (is (= #{"F" "G"} (merged-into "E")))
    (is (= #{"B" "C" "D" "E" "F" "G"} (merged-into "A")))
    (is (= 0 (merge-position "A")))
    (is (= 1 (merge-position "B")))
    (is (= 2 (merge-position "C")))
    (is (= 3 (merge-position "D")))
    (is (= 4 (merge-position "E")))
    (is (= 5 (merge-position "F")))
    (is (= 6 (merge-position "G"))))

  (testing "unmerge latest merge"
    (at-revision 7 (unmerge-node! "A" "E"))
    (is (= nil (merge-head "E")))
    (is (= #{"F" "G"} (merged-into "E")))
    (is (= #{"B" "C" "D"} (merged-into "A")))))

(deftest readable-merge-update
  (let [val (promise)]
    (at-revision 1
      (txn (jiraph.graph/compose (merge-node (layer :id) "A" "B")
                                 (update-in-node :people ["A"] adjoin {:foo 1})
                                 (fn [read]
                                   (do (deliver val (read (layer :people) ["B" :foo]))
                                       [])))))
    (is (= 1 @val))
    (is (= 1 (get-in-node :people ["A" :foo])))
    (is (= 1 (get-in-node :people ["B" :foo])))))

(deftest readable-update-merge
  (let [val (promise)]
    (at-revision 1
      (txn (jiraph.graph/compose (update-in-node :people ["A"] adjoin {:foo 1})
                                 (merge-node (layer :id) "A" "B")
                                 (fn [read]
                                   (do (deliver val (read (layer :people) ["B" :foo]))
                                       [])))))
    (is (= 1 @val))
    (is (= 1 (get-in-node :people ["A" :foo])))
    (is (= 1 (get-in-node :people ["B" :foo])))))

(deftest readable-merge-update-unmerge
  (let [val (promise)]
    (at-revision 1
      (txn (jiraph.graph/compose (merge-node (layer :id) "A" "B")
                                 (update-in-node :people ["A"] adjoin {:foo 1})
                                 (unmerge-node (layer :id) "A" "B")
                                 (fn [read]
                                   (do (deliver val (read (layer :people) ["B" :foo]))
                                       [])))))
    (is (= nil @val))
    (is (= 1   (get-in-node :people ["A" :foo])))
    (is (= nil (get-in-node :people ["B" :foo])))))

(deftest readable-merge-unmerge-merge
  (let [val1 (promise)
        val2 (promise)
        val3 (promise)]
    (at-revision 1
      (txn (jiraph.graph/compose (merge-node (layer :id) "A" "B")
                                 (fn [read]
                                   (do (deliver val1 (merged-into read (layer :id) "A"))
                                       []))
                                 (unmerge-node (layer :id) "A" "B")
                                 (fn [read]
                                   (do (deliver val2 (merged-into read (layer :id) "A"))
                                       [])))))
    (at-revision 2
      (txn (jiraph.graph/compose (merge-node (layer :id) "A" "B")
                                 (fn [read]
                                   (do (deliver val3 (merged-into read (layer :id) "A"))
                                       [])))))
    (is (= #{"B"} @val1))
    (is (= #{}    @val2))
    (is (= #{"B"} @val3))
    (is (= #{"B"} (merged-into (layer :id) "A")))))

(deftest edge-merging
  (at-revision 1 (assoc-in-node! :people ["A" :edges] {"B" {:foo 1} "C" {:foo 2}}))
  (at-revision 2 (merge-node! "C" "B"))

  (is (= {"C" {:foo 2}} (get-in-node :people ["A" :edges])))
  (is (= #{"A"} (get-incoming :people "C")))
  (is (= #{"A"} (get-incoming :people "B")))

  (at-revision 3 (unmerge-node! "C" "B"))
  (at-revision 4 (merge-node! "B" "C"))

  (is (= {"B" {:foo 1}} (get-in-node :people ["A" :edges])))
  (is (= #{"A"} (get-incoming :people "C")))

  (at-revision 5 (assoc-node! :people "D" {:a 1 :edges {"F" {:foo 3 :baz nil}}}))
  (at-revision 6 (assoc-node! :people "E" {:a 2 :edges {"G" {:foo 1 :bar 2 :baz 3}}}))
  (at-revision 7 (merge-node! "D" "E"))
  (at-revision 8 (merge-node! "G" "F"))

  (is (= {:a 1 :edges {"G" {:foo 3 :bar 2 :baz nil}}} (get-node :people "D")))
  (is (= {:a 1 :edges {"G" {:foo 3 :bar 2 :baz nil}}} (get-node :people "E")))
  (is (= #{"D"} (get-incoming :people "G")))
  (is (= #{"D"} (get-incoming :people "F")))

  (at-revision 9  (unmerge-node! "D" "E"))
  (at-revision 10 (unmerge-node! "G" "F"))

  (is (= {:a 1 :edges {"F" {:foo 3 :baz nil}}}      (get-node :people "D")))
  (is (= {:a 2 :edges {"G" {:foo 1 :bar 2 :baz 3}}} (get-node :people "E")))
  (is (= #{"E"} (get-incoming :people "G")))
  (is (= #{"D"} (get-incoming :people "F"))))

(deftest deleted-edge-merging-opposite-direction
  (at-revision 1 (assoc-node! :people "A" {:edges {"C" {:deleted true}}}))
  (at-revision 2 (assoc-node! :people "B" {:edges {"D" {:deleted false}}}))
  (at-revision 3 (merge-node! "A" "B"))
  (at-revision 4 (merge-node! "D" "C"))

  (is (= {:edges {"D" {:deleted false}}} (get-node :people "A")))
  (is (= {:edges {"D" {:deleted false}}} (get-node :people "B")))
  (is (= {"A" true} (get-incoming-map :people "C")))
  (is (= {"A" true} (get-incoming-map :people "D"))))

(deftest deleted-edge-merging-same-direction
  (at-revision 1 (assoc-node! :people "A" {:edges {"C" {:deleted true}}}))
  (at-revision 2 (assoc-node! :people "B" {:edges {"D" {:deleted false}}}))
  (at-revision 3 (merge-node! "A" "B"))
  (at-revision 4 (merge-node! "C" "D"))

  (is (= {:edges {"C" {:deleted false}}} (get-node :people "A")))
  (is (= {:edges {"C" {:deleted false}}} (get-node :people "B")))
  (is (= {"A" true} (get-incoming-map :people "C")))
  (is (= {"A" true} (get-incoming-map :people "D"))))
