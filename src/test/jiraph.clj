(ns test.jiraph
  (:use jiraph)
  (:use test.utils)
  (:use protobuf)
  (:use clojure.test))

(use-fixtures :each setup-db-path)

(defprotobuf Node jiraph.Protos Node)
(defprotobuf Edge jiraph.Protos Edge)

(defn upcase-data [map]
  (let [data #^String (map :data)]
    (assoc map :data (.toUpperCase data))))

(deftest graph-access
  (let [g (open-graph *db-path* Node EdgeList)]
    (testing "nodes"
             (add-node! g :id 1 :type "person" :data "foo")
             (let [node (get-node g 1)]
               (is (= 1        (:id node)))
               (is (= "foo"    (:data node)))
               (is (= "person" (:type node))))

             (assoc-node! g :id 1 :data "zap")
             (let [node (get-node g 1)]
               (is (= 1        (:id node)))
               (is (= "zap"    (:data node)))
               (is (= "person" (:type node))))

             (update-node! g 1 #(assoc % :data "bar"))
             (let [node (get-node g 1)]
               (is (= 1        (:id node)))
               (is (= "bar"    (:data node)))
               (is (= "person" (:type node))))

             (delete-node! g 1)
             (is (= nil (get-node g 1)))

             )
    (testing "edges"
             (add-edge! g :from-id 1 :to-id 2 :type "friend")
             (add-edge! g :from-id 1 :to-id 4 :type "friend")
             (add-edge! g :from-id 1 :to-id 5 :type "enemy")
             (let [edges (get-edges g 1 "friend")
                   [e1 e2] edges]
               (is (= 2 (count edges)))
               (is (= e1 {:from-id 1, :to-id 2, :type "friend"}))
               (is (= e2 {:from-id 1, :to-id 4, :type "friend"})))
             (let [edges (get-edges g 1 "enemy")
                   edge  (first edges)]
               (is (= 1 (count edges)))
               (is (= edge {:from-id 1, :to-id 5, :type "enemy"})))

             (assoc-edge! g :from-id 1 :to-id 5 :type "enemy" :data "baz")
             (let [edges (get-edges g 1 "enemy")
                   edge  (first edges)]
               (is (= 1 (count edges)))
               (is (= edge {:from-id 1, :to-id 5, :type "enemy", :data "baz"})))

             (update-edge! g 1 5 "enemy" upcase-data)
             (let [edges (get-edges g 1 "enemy")
                   edge  (first edges)]
               (is (= 1 (count edges)))
               (is (= edge {:from-id 1, :to-id 5, :type "enemy", :data "BAZ"})))

             (delete-edge! g 1 5 "enemy")
             (let [edges (get-edges g 1 "enemy")]
               (is (empty? edges)))
             )
    ))

