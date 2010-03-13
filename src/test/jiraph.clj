(ns test.jiraph
  (:use jiraph)
  (:use protobuf)
  (:use clojure.test))

(defn append-actions [layer id & args]
  (let [actions  (switch-layer layer :actions)
        callback (:callback (meta layer))]
    (conj-node! actions id (layer :name) [(into [callback id] args)])))

(defgraph graph
  :path "/tmp/jiraph-test" :proto jiraph.Proto$Node :create true :bnum 1000000
  (layer :friends :store-length-on-append true)
  (layer :enemies :post-write append-actions)
  (layer :actions :append-only true :proto nil))

(defn clear-graph [f]
  (doseq [layer (vals graph)]
    (jiraph.tc/db-truncate layer))
  (f))

(use-fixtures :each clear-graph)

(defn upcase-data [m]
  (let [data #^String (m :data)]
    (assoc m :data (.toUpperCase data))))

(deftest graph-access
  (testing "nodes"
    (let [layer (graph :friends)]
      (add-node! layer 1 :type "person" :data "foo")
      (let [node (get-node layer 1)]
        (is (= 1        (:id node)))
        (is (= "foo"    (:data node)))
        (is (= "person" (:type node))))

      (assoc-node! layer 1 :data "zap")
      (let [node (get-node layer 1)]
        (is (= 1        (:id node)))
        (is (= "zap"    (:data node)))
        (is (= "person" (:type node))))

      (update-node! layer 1 #(assoc % :data "bar"))
      (let [node (get-node layer 1)]
        (is (= 1        (:id node)))
        (is (= "bar"    (:data node)))
        (is (= "person" (:type node))))

      (conj-node! layer 1 :data "baz")
      (let [node (get-node layer 1)
            len  (first (:_len node))]
        (is (= 1        (:id node)))
        (is (= "baz"    (:data node)))
        (is (= "person" (:type node)))
        (is (< 0        len))
        ; now fetch the previous node using len
        (let [node (get-node layer 1 len)]
          (is (= 1        (:id node)))
          (is (= "bar"    (:data node)))
          (is (= "person" (:type node)))))

      (delete-node! layer 1)
      (is (= nil (get-node layer 1)))

      ))
  (testing "edges"
    (assoc-edge! (graph :enemies) 1 5 :data "arch-nemesis")
    (let [edges (get-edges (graph :enemies) 1)]
      (is (= 1 (count edges)))
      (is (= (edges 5) {:to-id 5, :data "arch-nemesis"})))

    (assoc-edge! (graph :enemies) 1 5 :data "baz")
    (let [edges (get-edges (graph :enemies) 1)]
      (is (= 1 (count edges)))
      (is (= (edges 5) {:to-id 5, :data "baz"})))

    (update-edge! (graph :enemies) 1 5 upcase-data)
    (let [edges (get-edges (graph :enemies) 1)]
      (is (= 1 (count edges)))
      (is (= (edges 5) {:to-id 5, :data "BAZ"})))

    (delete-edge! (graph :enemies) 1 5)
    (let [edges (get-edges (graph :enemies) 1)]
      (is (empty? edges)))

    (conj-edge! (graph :friends) 1 2 :data "since high-school")
    (conj-edge! (graph :friends) 1 4 :data "from work")
    (let [edges (get-edges (graph :friends) 1)]
      (is (= 2 (count edges)))
      (is (= (edges 2) {:to-id 2, :data "since high-school"}))
      (is (= (edges 4) {:to-id 4, :data "from work"})))

    (conj-edge! (graph :friends) 1 2 :data "ZAP!")
    (conj-edge! (graph :friends) 1 4 :data "ZAP!")
    (conj-edge! (graph :friends) 1 8 :data "spiderman")
    (let [node  (get-node (graph :friends) 1)
          edges (get-edges node)
          len   (node :_len)]
      (is (= 3 (count edges)))
      (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
      (is (= (edges 4) {:to-id 4, :data "ZAP!"}))
      (is (= (edges 8) {:to-id 8, :data "spiderman"}))
      (is (=  5 (count len)))
      (is (= -1 (first len)))
      ; back through history
      (let [edges (get-edges (graph :friends) 1 (len 1))]
        (is (= 1 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "since high-school"})))
      (let [edges (get-edges (graph :friends) 1 (len 2))]
        (is (= 2 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "since high-school"}))
        (is (= (edges 4) {:to-id 4, :data "from work"})))
      (let [edges (get-edges (graph :friends) 1 (len 3))]
        (is (= 2 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
        (is (= (edges 4) {:to-id 4, :data "from work"})))
      (let [edges (get-edges (graph :friends) 1 (len 4))]
        (is (= 2 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
        (is (= (edges 4) {:to-id 4, :data "ZAP!"})))
      ))
  (testing "callbacks"
    (add-node!    (graph :enemies) 11 :type "nemesis" :data "evil")
    (add-node!    (graph :enemies) 15 :type "rival"   :data "bad")
    (conj-node!   (graph :enemies) 11 :type "arch-nemesis")
    (assoc-node!  (graph :enemies) 15 :type "arch-rival")
    (delete-node! (graph :enemies) 11)
    (delete-node! (graph :enemies) 15)
    (let [actions (get-node (graph :actions) 11)]
      (is (= 3 (count (actions :enemies))))
      (is (= [:post-add 11 {:id 11, :type "nemesis", :data "evil"}] ((actions :enemies) 0)))
      (is (= [:post-append 11 {:type "arch-nemesis"}]               ((actions :enemies) 1)))
      (is (= [:post-delete 11]                                      ((actions :enemies) 2))))
    (let [actions (get-node (graph :actions) 15)]
      (is (= 3 (count (actions :enemies))))
      (is (= [:post-add 15 {:id 15, :type "rival", :data "bad"}]         ((actions :enemies) 0)))
      (is (= [:post-update 15 {:id 15, :type "rival", :data "bad"}
                              {:id 15, :type "arch-rival", :data "bad"}] ((actions :enemies) 1)))
      (is (= [:post-delete 15]                                           ((actions :enemies) 2))))
    ))
