(ns test.jiraph
  (:use jiraph)
  (:use protobuf)
  (:use clojure.test))

(defn append-actions [layer id & args]
  (conj-node! :actions id layer [(into [jiraph/*callback* id] args)]))

(defgraph proto-graph
  :path "/tmp/jiraph-proto-test" :proto test.jiraph.Proto$Node :create true :bnum 1000000
  (layer :friends :append-only true)
  (layer :enemies :auto-compact true :post-write append-actions)
  (layer :actions :append-only true :proto nil))

(defgraph graph
  :path "/tmp/jiraph-test" :create true :bnum 1000000
  (layer :friends :append-only true)
  (layer :enemies :auto-compact false :post-write append-actions)
  (layer :actions :append-only true))


(defmacro with-each-graph [graphs & body]
  `(doseq [g# ~graphs]
     (with-graph g#
       ~@body)))

(defn clear-graphs [f]
  (with-each-graph [graph proto-graph]
    (truncate-graph!))
  (f))

(use-fixtures :each clear-graphs)

(defn upcase-data [m]
  (let [data #^String (m :data)]
    (assoc m :data (.toUpperCase data))))

(deftest layer-meta-accessors
  (with-each-graph [graph proto-graph]
    (assoc-layer-meta! :friends :version 4)
    (is (= {:version 4} (layer-meta :friends)))))

(deftest field-to-layer-map
  (is (= {:data :friends, :type :friends} (field-to-layer proto-graph :friends :enemies))))

(deftest graph-access
  (with-each-graph [graph proto-graph]
    (testing "nodes"
      (is (not (node-exists? :enemies 1)))
      (add-node! :enemies 1 :type "person" :data "foo")
      (is (node-exists? :enemies 1))
      (let [node (get-node :enemies 1)]
        (is (= 1        (:id node)))
        (is (= "foo"    (:data node)))
        (is (= "person" (:type node))))

      (assoc-node! :enemies 1 :data "zap")
      (let [node (get-node :enemies 1)]
        (is (= 1        (:id node)))
        (is (= "zap"    (:data node)))
        (is (= "person" (:type node))))

      (update-node! :enemies 1 #(assoc % :data "bar"))
      (let [node (get-node :enemies 1)]
        (is (= 1        (:id node)))
        (is (= "bar"    (:data node)))
        (is (= "person" (:type node))))

      (conj-node! :friends 1 :data "bar" :type "person")
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

      (testing "cannot change id of node"
        (let [len (node-len :friends 1)]
          (conj-node! :friends 1 :id 5) ; should not change id
          (is (= len (node-len :friends 1)))
          (is (= 1 (:id (get-node :friends 1)))))

        (assoc-node! :enemies 1 :id 5)
        (is (= 1 (:id (get-node :enemies 1))))

        (add-node! :enemies 5 :id 6 :data "bar")
        (is (= 5 (:id (get-node :enemies 5))))
        )

      (delete-node! :enemies 1)
      (is (not (node-exists? :enemies 1)))
      (is (= nil (get-node :enemies 1)))
      )
    (testing "edges"
      (add-node! :enemies 1 :data "bar")
      (assoc-edge! :enemies 1 5 :data "arch-nemesis")
      (let [edges (get-edges :enemies 1)]
        (is (= 1 (count edges)))
        (is (= (edges 5) {:to-id 5, :data "arch-nemesis"})))

      (assoc-edge! :enemies 3 6)
      (let [edges (get-edges :enemies 3)]
        (is (= 1 (count edges)))
        (is (= (edges 6) {:to-id 6})))

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
        (is (= 6 (count len)))
        (is (< 0 (first len)))
        ; back through history
        (let [edges (get-edges :friends 1 (len 2))]
          (is (= 1 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "since high-school"})))
        (let [edges (get-edges :friends 1 (len 3))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "since high-school"}))
          (is (= (edges 4) {:to-id 4, :data "from work"})))
        (let [edges (get-edges :friends 1 (len 4))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
          (is (= (edges 4) {:to-id 4, :data "from work"})))
        (let [edges (get-edges :friends 1 (len 5))]
          (is (= 2 (count edges)))
          (is (= (edges 2) {:to-id 2, :data "ZAP!"}))
          (is (= (edges 4) {:to-id 4, :data "ZAP!"})))
        ))
    (testing "auto-compact"
      (conj-node! :enemies 1 :data "bar" :type "foo")
      (let [len (node-len :enemies 1)]
        (conj-node! :enemies 1 :data "baz")
        (if (opt :enemies :auto-compact)
          (is (= len (node-len :enemies 1)))
          (is (< len (node-len :enemies 1))))
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
      ))
)