(ns test.jiraph
  (:use jiraph)
  (:use protobuf)
  (:use clojure.test))

(defn append-actions [layer id & args]
  (conj-node! :actions id layer [(into [jiraph/*callback* id] args)]))

(defgraph proto-graph
  :path "/tmp/jiraph-proto-test" :proto test.jiraph.Proto$Node :create true :bnum 1000000
  (layer :friends :store-length-on-append true)
  (layer :enemies :post-write append-actions)
  (layer :actions :append-only true :proto nil))

(defgraph graph
  :path "/tmp/jiraph-test" :create true :bnum 1000000
  (layer :friends :store-length-on-append true)
  (layer :enemies :post-write append-actions)
  (layer :actions :append-only true))

(defn clear-graph [f]
  (doseq [layer (concat (vals graph) (vals proto-graph))]
    (jiraph.tc/db-truncate layer))
  (f))

(use-fixtures :each clear-graph)

(defn upcase-data [m]
  (let [data #^String (m :data)]
    (assoc m :data (.toUpperCase data))))

(deftest layer-meta-accessors
  (with-graph graph
    (assoc-layer-meta! :friends :version 4)
    (is (= {:version 4} (layer-meta :friends)))))

(deftest graph-access
  (doseq [g [graph proto-graph]]
  (with-graph g
    (testing "nodes"
      (add-node! :friends 1 :type "person" :data "foo")
      (let [node (get-node :friends 1)]
        (is (= 1        (:id node)))
        (is (= "foo"    (:data node)))
        (is (= "person" (:type node))))

      (assoc-node! :friends 1 :data "zap")
      (let [node (get-node :friends 1)]
        (is (= 1        (:id node)))
        (is (= "zap"    (:data node)))
        (is (= "person" (:type node))))

      (update-node! :friends 1 #(assoc % :data "bar"))
      (let [node (get-node :friends 1)]
        (is (= 1        (:id node)))
        (is (= "bar"    (:data node)))
        (is (= "person" (:type node))))

      (conj-node! :friends 1 :data "baz")
      (let [node (get-node :friends 1)
            len  (first (:_len node))]
        (is (= 1        (:id node)))
        (is (= "baz"    (:data node)))
        (is (= "person" (:type node)))
        (is (< 0        len))
        ; now fetch the previous node using len
        (let [node (get-node :friends 1 len)]
          (is (= 1        (:id node)))
          (is (= "bar"    (:data node)))
          (is (= "person" (:type node)))))

      (delete-node! :friends 1)
      (is (= nil (get-node :friends 1)))
      )
    (testing "edges"
      (add-node! :enemies 1 :data "bar")
      (assoc-edge! :enemies 1 5 :data "arch-nemesis")
      (let [edges (get-edges :enemies 1)]
        (is (= 1 (count edges)))
        (is (= (edges 5) {:to-id 5, :data "arch-nemesis"})))

      (assoc-edge! :enemies 1 5 :data "baz")
      (let [edges (get-edges :enemies 1)]
        (is (= 1 (count edges)))
        (is (= (edges 5) {:to-id 5, :data "baz"})))

      (update-edge! :enemies 1 5 upcase-data)
      (let [edges (get-edges :enemies 1)]
        (is (= 1 (count edges)))
        (is (= (edges 5) {:to-id 5, :data "BAZ"})))

      (delete-edge! :enemies 1 5)
      (let [edges (get-edges :enemies 1)]
        (is (empty? edges)))

      (conj-edge! :friends 1 2 :data "since high-school")
      (conj-edge! :friends 1 4 :data "from work")
      (let [edges (get-edges :friends 1)]
        (is (= 2 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "since high-school"}))
        (is (= (edges 4) {:to-id 4, :data "from work"})))

      (conj-edge! :friends 1 2 :data "ZAP!")
      (conj-edge! :friends 1 4 :data "ZAP!")
      (conj-edge! :friends 1 8 :data "spiderman")
      (let [node  (get-node :friends 1)
            edges (:edges node)
            len   (node :_len)]
        (is (= 3 (count edges)))
        (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
        (is (= (edges 4) {:to-id 4, :data "ZAP!"}))
        (is (= (edges 8) {:to-id 8, :data "spiderman"}))
        (is (=  5 (count len)))
        (is (= -1 (first len)))
        ; back through history
        (let [edges (get-edges :friends 1 (len 1))]
          (is (= 1 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "since high-school"})))
        (let [edges (get-edges :friends 1 (len 2))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "since high-school"}))
          (is (= (edges 4) {:to-id 4, :data "from work"})))
        (let [edges (get-edges :friends 1 (len 3))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
          (is (= (edges 4) {:to-id 4, :data "from work"})))
        (let [edges (get-edges :friends 1 (len 4))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
          (is (= (edges 4) {:to-id 4, :data "ZAP!"})))
        ))
    (testing "callbacks"
      (add-node!    :enemies 11 :type "nemesis" :data "evil")
      (add-node!    :enemies 15 :type "rival"   :data "bad")
      (conj-node!   :enemies 11 :type "arch-nemesis")
      (assoc-node!  :enemies 15 :type "arch-rival")
      (delete-node! :enemies 11)
      (delete-node! :enemies 15)
      (let [actions (get-node :actions 11)]
        (is (= 3 (count (actions :enemies))))
        (is (= [:post-add 11 {:id 11, :type "nemesis", :data "evil"}] ((actions :enemies) 0)))
        (is (= [:post-append 11 {:type "arch-nemesis"}]               ((actions :enemies) 1)))
        (is (= [:post-delete 11]                                      ((actions :enemies) 2))))
      (let [actions (get-node :actions 15)]
        (is (= 3 (count (actions :enemies))))
        (is (= [:post-add 15 {:id 15, :type "rival", :data "bad"}]         ((actions :enemies) 0)))
        (is (= [:post-update 15 {:id 15, :type "rival", :data "bad"}
                                {:id 15, :type "arch-rival", :data "bad"}] ((actions :enemies) 1)))
        (is (= [:post-delete 15]                                           ((actions :enemies) 2))))
      )))
)