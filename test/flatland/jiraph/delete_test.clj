(ns flatland.jiraph.delete-test
  (:use clojure.test flatland.jiraph.core flatland.jiraph.delete)
  (:require [flatland.jiraph.masai-layer :as masai]))

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

  (at-revision 2 (delete-node! "A"))
  (is (= {:foo 1 :deleted true} (get-node :people "A")))
  (is (node-deleted? "A"))

  (at-revision 3 (undelete-node! "A"))
  (is (= {:foo 1} (get-node :people "A")))
  (is (not (node-deleted? "A"))))
