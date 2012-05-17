(ns jiraph.delete-test
  (:use clojure.test jiraph.core jiraph.delete)
  (:require [jiraph.masai-layer :as masai]))

(defn empty-graph [f]
  (let [id-layer (masai/make-temp)]
    (with-graph {:id     id-layer
                 :people (deletable-layer (masai/make-temp) id-layer)}
      (f))))

(use-fixtures :each empty-graph)

(deftest delete-and-undelete-node
  (at-revision 1 (assoc-node! :people "A" {:foo 1}))
  (is (= {:foo 1} (get-node :people "A")))
  (is (not (node-deleted? "A")))

  (delete-node! "A")
  (is (= {:foo 1 :deleted true} (get-node :people "A")))
  (is (node-deleted? "A"))

  (undelete-node! "A")
  (is (= {:foo 1} (get-node :people "A")))
  (is (not (node-deleted? "A"))))