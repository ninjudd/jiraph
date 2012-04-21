(ns jiraph.merge-test
  (:use clojure.test jiraph.core)
  (:require [jiraph.masai-layer :as masai]))

(defn empty-graph [f]
  (with-graph {:meta   (masai/make-temp)
               :people (masai/make-temp)}
    (f)))

(use-fixtures :each empty-graph)

(deftest merging
  (testing "nothing is merged initially"
    (is (empty? (merged-into "A")))
    (is (empty? (merged-into "B"))))

  (testing "merge two nodes"
    (at-revision 1 (merge-node! "A" "B"))
    (is (= #{"B"} (merged-into "A"))))

  (testing "cannot re-merge tail"
    (is (thrown-with-msg? Exception #"already merged"
          (merge-node! "C" "B"))))

  (testing "cannot merge into non-head"
    (is (thrown-with-msg? Exception #"already merged"
          (merge-node! "B" "C"))))

  (testing "merge multiple nodes into a single head"
    (at-revision 2 (merge-node! "A" "C"))
    (at-revision 3 (merge-node! "A" "D"))
    (is (= #{"B" "C" "D"} (merged-into "A"))))

  (testing "merge two chains together"
    (at-revision 4 (merge-node! "E" "F"))
    (at-revision 5 (merge-node! "E" "G"))
    (is (= #{"F" "G"} (merged-into "E")))
    (at-revision 6 (merge-node! "A" "E"))
    (is (= #{"F" "G"} (merged-into "E")))
    (is (= #{"B" "C" "D" "E" "F" "G"} (merged-into "A"))))
    
  (testing "unmerge latest merge"
    (at-revision 7 (unmerge-node! "A" "E"))
    (is (= #{"F" "G"} (merged-into "E")))
    (is (= #{"B" "C" "D"} (merged-into "A")))))
