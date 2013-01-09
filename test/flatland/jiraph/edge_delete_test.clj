(ns flatland.jiraph.edge-delete-test
  (:use clojure.test flatland.jiraph.core flatland.jiraph.edge-delete)
  (:require [flatland.jiraph.masai-layer :as masai]
            [flatland.useful.map :refer [dissoc-in*]]))

(defn empty-graph [f]
  (let [id-layer (masai/make-temp)]
    (with-graph {:id     id-layer
                 :people (make (masai/make-temp) id-layer)}
      (f))))

(use-fixtures :each empty-graph)

(deftest delete-and-undelete-node
  (let [A {:foo 1 :edges {"B" {:bar 2} "C" {:bar 3}}}
        B {:foo 2 :edges {"A" {:bar 4} "C" {:bar 5}}}]
    (at-revision 1 (assoc-node! :people "A" A))
    (at-revision 1 (assoc-node! :people "B" B))
    (is (= A (get-node :people "A")))
    (is (= B (get-node :people "B")))
    (is (not (edges-deleted? "A")))
    
    (at-revision 2 (delete-edges! "A"))
    (is (= (assoc A :edges {}) (get-node :people "A")))
    (is (= (dissoc-in* B [:edges "A"]) (get-node :people "B")))
    (is (edges-deleted? "A"))
    
    (at-revision 3 (undelete-edges! "A"))
    (is (= A (get-node :people "A")))
    (is (= B (get-node :people "B")))
    (is (not (edges-deleted? "A")))))
