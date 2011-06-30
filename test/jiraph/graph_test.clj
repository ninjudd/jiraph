(ns jiraph.graph-test
  (:use clojure.test jiraph.graph)
  (:require [jiraph.masai-layer :as bal]
            [jiraph.stm-layer :as stm]
            [masai.tokyo :as tokyo]
            [cereal.reader :as raf]
            [cereal.protobuf :as paf])
  (:import [jiraph Test$Node]))

(def all [:tr :tp :stm])

(defn make-graph []
  {:tr (make-layer "/tmp/jiraph-test-tokyo-reader")
   :tp (bal/make (tokyo/make {:path "/tmp/jiraph-test-tokyo-protobuf" :create true}) (paf/make Test$Node))
   :stm (stm/make)})

(deftest add-node
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "add-node! throws exception and doesn't overwrite existing node"
        (let [node {:foo 2 :bar "three"}] (is (= node (add-node! layer-name "1" node)))
          (is (= (assoc node :id "1") (get-node layer-name "1")))
          (is (thrown-with-msg? java.io.IOException #"already exists"
                (add-node! layer-name "1" {:foo 8})))
          (is (= (assoc node :id "1") (get-node layer-name "1"))))))))

(deftest node-info
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "node-ids, node-count and node-exists?"
        (is (add-node! layer-name "1" {:foo 0}))
        (is (= #{"1"} (set (node-ids layer-name))))
        (is (= 1 (node-count layer-name)))
        (is (node-exists? layer-name "1"))
        (doseq [id ["8" "9" "234"]]
          (is (not (node-exists? layer-name id))))))))

(deftest update-node
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "update-node! supports artitrary functions"
        (let [node1 {:foo 2 :bar "three" :baz [1 2 3]}
              node2 {:foo 2 :bar "three"}]
          (is (add-node! layer-name "1" node2))
          (is (= [node2 node1] (update-node! layer-name "1" assoc :baz [1 2 3])))
          (is (= [node1 node2] (update-node! layer-name "1" dissoc :baz)))
          (is (= (assoc node2 :id "1") (get-node layer-name "1")))
          (let [node3 {:foo 2 :bar "three" :baz [5]}]
            (is (= [node2 node3] (update-node! layer-name "1" assoc :baz [5])))
            (is (= (assoc node3 :id "1") (get-node layer-name "1")))
            (let [node4 {:foo 2 :baz [5]}]
              (is (= [node3 node4] (update-node! layer-name "1" select-keys [:foo :baz])))
              (is (= (assoc node4 :id "1") (get-node layer-name "1"))))))))))

(deftest caching
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "with-caching"
        (let [get-node-without-caching get-node]
          (with-caching
            (at-revision 100 (is (add-node! layer-name "3" {:bar "cat" :baz [5]})))
            (at-revision 101 (is (append-node! layer-name "3" {:baz [8]})))
            (is (not= get-node-without-caching get-node))
            (is (= {:id "3" :bar "cat" :baz [5 8] :rev 101} (get-node layer-name "3")))
            (at-revision 100
                         (is (= {:id "3" :bar "cat" :baz [5] :rev 100} (get-node layer-name "3"))))))))))

(deftest transactions
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "transactions"
        (let [node {:foo 7 :bar "seven"}]
          (with-transaction layer-name
            (is (= node (add-node! layer-name "7" node)))
            (is (= (assoc node :id "7") (get-node layer-name "7")))
            (abort-transaction))
          (is (= nil (get-node layer-name "7")))
          (is (thrown? Error
                       (with-transaction layer-name
                         (is (= node (add-node! layer-name "7" node)))
                         (is (= (assoc node :id "7") (get-node layer-name "7")))
                         (throw (Error.)))))
          (is (= nil (get-node layer-name "7")))
          (with-transaction layer-name
            (is (= node (add-node! layer-name "7" node))))
          (is (= (assoc node :id "7") (get-node layer-name "7"))))))))

(deftest properties
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "layer-wide properties"
        (is (= nil (get-property layer-name :foo)))
        (is (= [1 2 3] (set-property! layer-name :foo [1 2 3])))
        (is (= [1 2 3] (get-property layer-name :foo)))
        (is (= [1 2 3 5] (update-property! layer-name :foo conj 5)))
        (is (= [1 2 3 5] (get-property layer-name :foo)))))))

(deftest incoming
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)
      (testing "keeps track of incoming edges"
        (is (empty? (get-incoming layer-name "1")))
        (is (add-node! layer-name "4" {:edges {"1" {:a "one"}}}))
        (is (= #{"4"} (get-incoming layer-name "1")))
        (is (add-node! layer-name "5" {:edges {"1" {:b "two"}}}))
        (is (= #{"4" "5"} (get-incoming layer-name "1")))
        (is (append-node! layer-name "5" {:edges {"1" {:deleted true}}}))
        (is (= #{"4"} (get-incoming layer-name "1")))
        (is (assoc-node! layer-name "4" {:edges {"2" {:a "1"} "3" {:b "2"}}}))
        (is (= #{"4"} (get-incoming layer-name "2")))
        (is (= #{"4"} (get-incoming layer-name "3")))))))

(deftest edge
  (with-graph
    (into {} (for [[k v] (make-graph)] [k (with-meta v {:single-edge true})]))
    (with-each-layer all
      (truncate! layer-name)
      (testing "can handle :edge"
        (is (empty? (get-incoming layer-name "1")))
        (is (add-node! layer-name "4" {:edge {:id "1"}}))
        (is (= #{"4"} (get-incoming layer-name "1")))
        (is (update-node! layer-name "4" (constantly {:edge {:id "2"}})))
        (is (= #{"4"} (get-incoming layer-name "2")))
        (is (update-node! layer-name "4" (constantly {:edge {:id "2" :deleted true}})))
        (is (= #{} (get-incoming layer-name "2")))))))

(deftest revisions
  (with-graph (make-graph)
    (with-each-layer all
      (truncate! layer-name)

      (testing "append-node! supports viewing old revisions"
        (let [node  {:bar "cat" :baz [5] :rev 100}
              attrs {:baz [8] :rev 101}]
          (at-revision 100
            (is (= node (append-node! layer-name "3" (dissoc node :rev))))
            (is (= (assoc node :id "3") (get-node layer-name "3"))))
          (at-revision 101
            (is (= attrs (append-node! layer-name "3" (dissoc attrs :rev))))
            (is (= {:id "3" :bar "cat" :baz [5 8] :rev 101} (get-node layer-name "3"))))
          (at-revision 100
            (is (= (assoc node :id "3") (get-node layer-name "3"))))))

      (testing "get-node returns nil if node didn't exist at-revision"
        (at-revision 99
          (is (= nil (get-node layer-name "3")))))

      (testing "revisions and all-revisions returns an empty list for nodes without revisions"
        (is (= () (get-revisions layer-name "1")))
        (is (= () (get-all-revisions layer-name "1"))))

      (testing ":rev property stores max committed revision"
        (at-revision 102
          (with-transaction layer-name
            (add-node! layer-name "8" {:foo 8}))
          (is (= 8 (:foo (get-node layer-name "8"))))
          (is (= 102 (get-property layer-name :rev)))))

      (testing "past revisions are ignored inside of transactions"
        (at-revision 101
          (with-transaction layer-name
            (add-node! layer-name "8" {:foo 9})))
        (is (= 8 (:foo (get-node layer-name "8")))))



      (testing "keeps track of incoming edges inside at-revision"
        (at-revision 199 (is (= nil (get-incoming layer-name "11"))))
        (at-revision 200
          (is (add-node! layer-name "10" {:edges {"11" {:a "one"}}})))

        (is (= #{"10"} (get-incoming layer-name "11")))
        (at-revision 199 (is (= nil (get-incoming layer-name "11"))))

        (at-revision 201
          (is (add-node! layer-name "12" {:edges {"11" {:a "one"}}})))

        (is (= #{"10" "12"} (get-incoming layer-name "11")))
        (at-revision 199 (is (= nil (get-incoming layer-name "11"))))
        (at-revision 200 (is (= #{"10"} (get-incoming layer-name "11"))))

        (at-revision 202
          (is (add-node! layer-name "13" {:edges {"11" {:a "one"}}})))

        (is (= #{"10" "12" "13"} (get-incoming layer-name "11")))
        (at-revision 199 (is (= nil (get-incoming layer-name "11"))))
        (at-revision 200 (is (= #{"10"} (get-incoming layer-name "11"))))
        (at-revision 201 (is (= #{"10" "12"} (get-incoming layer-name "11"))))))))

(deftest compact-node
  (with-graph (make-graph)
    (with-each-layer [:tp :tr]
      (truncate! layer-name)
      (testing "compact-node! removes revisions but leaves all-revisions"
        (let [old {:bar "cat" :baz [5 8] :rev 101}
              new {:bar "cat", :baz [5 8]}]
          (at-revision 100 (add-node! layer-name "3" {:bar "cat" :baz [5]}))
          (at-revision 101 (append-node! layer-name "3" {:baz [8]}))
          (is (= '(100 101) (get-revisions layer-name "3")))
          (is (= '(100 101) (get-all-revisions layer-name "3")))
          (is (= [old new] (compact-node! layer-name "3")))
          (is (= () (get-revisions layer-name "3")))
          (is (= '(100, 101) (get-all-revisions layer-name "3"))))))))

(deftest assoc-node
  (with-graph (make-graph)
    (with-each-layer [:tp :tr]
      (truncate! layer-name)
      (testing "assoc-node! modifies specific attributes"
        (let [old {:foo 2 :bar "three"}
              new {:foo 54 :bar "three" :baz [1 2 3]}]
          (is (add-node! layer-name "1" old))
          (is (= [old new] (assoc-node! layer-name "1" {:foo 54 :baz [1 2 3]})))
          (is (= (assoc new :id "1") (get-node layer-name "1")))))

      (testing "assoc-node! creates node if it doesn't exist"
        (let [node {:foo 9 :bar "the answer"}]
          (is (= [nil node] (assoc-node! layer-name "2" {:foo 9 :bar "the answer"})))
          (is (= (assoc node :id "2") (get-node layer-name "2")))))

      (testing "assoc-node wipes edges"
        (is (assoc-node! layer-name "4" {:edges {"1" {:a "2"}}}))
        (is (= #{"4"} (get-incoming layer-name "1")))
        (is (assoc-node! layer-name "4" {:edges {"2" {:a "1"} "3" {:b "2"}}}))
        (is (empty? (get-incoming layer-name "1")))))))

(deftest adhere-schema
  (with-graph
    (into {} (for [[k v] (make-graph)]
               [k (with-meta v {:types #{:foo :bar} :edge-types #{:bar}})]))
    (with-each-layer all
      (truncate! layer-name)
      (testing "adheres to the schema"
        (is (add-node! layer-name "bar-1" {:a "b"}))
        (is (add-node! layer-name "foo-1" {:edges {"bar-1" {:b "2"}}}))
        (is (thrown-with-msg? Exception #"doesn't match"
              (add-node! layer-name "baz-1" {:a "b"})))
        (is (thrown-with-msg? Exception #"doesn't match"
              (update-node! layer-name "baz-1" {:a "b"})))
        (is (thrown-with-msg? Exception #"doesn't match"
              (append-node! layer-name "baz-1" {:a "b"}))))

      (testing "can find layers with a specific type"
        (is (= [:tr :tp :stm] (layers-with-type :foo)))))))

(deftest edges-valid
  (with-graph {:stm1 (stm/make)
               :stm2 (with-meta (stm/make) {:single-edge true})}
    (map truncate! (keys *graph*))
    (testing "behaves properly when :single-edge is false"
      (is (not (edges-valid? :stm1 {:edge {:id "1"}})))
      (is (edges-valid? :stm1 {:edges {"1" {:a "b"}}})))
    (testing "behaves properly when :single-edge is true"
      (is (edges-valid? :stm2 {:edge {:id "1"}}))
      (is (not (edges-valid? :stm2 {:edges {"1" {:a "b"}}}))))))

(deftest test-fields-and-schema
  (with-graph {:a (with-meta (bal/make (tokyo/make {:path "/tmp/jiraph-test-a" :create true})
                                       (paf/make Test$Node))
                    {:types #{:foo :bar}})
               :b (with-meta (bal/make (tokyo/make {:path "/tmp/jiraph-test-b" :create true})
                                       (raf/make (with-meta {:foo 1 :bap 2}
                                                   {:foo {:type :int} :bap {:type :double}})))
                    {:types #{:baz :bar}})
               :c (with-meta (bal/make (tokyo/make {:path "/tmp/jiraph-test-c" :create true})
                                       (raf/make {:one 1 :two 2 :foo 3}))
                    {:types #{:foo :bam}})}
    (is (= {:baz {:repeated true, :type :int},
            :bar {:repeated false, :type :string},
            :foo {:repeated false, :type :int}}
           (fields :a)))
    (is (= {:one {:c nil},
            :two {:c nil},
            :foo {:c nil, :a {:repeated false, :type :int}},
            :bar {:a {:repeated false, :type :string}},
            :baz {:a {:repeated true, :type :int}}}
           (schema :foo)))
    (is (= {:bap {:b {:type :double}},
            :foo {:b {:type :int}, :a {:repeated false, :type :int}},
            :bar {:a {:repeated false, :type :string}},
            :baz {:a {:repeated true, :type :int}}}
           (schema :bar)))))

