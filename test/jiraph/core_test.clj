(ns jiraph.core-test
  (:use clojure.test jiraph.core
        [useful.utils :only [adjoin]]
        [retro.core :as retro :only [dotxn]])
  (:require [jiraph.stm-layer :as stm]
            [jiraph.layer :as layer]
            [jiraph.null-layer :as null]
            [jiraph.masai-layer :as masai]
            [jiraph.masai-sorted-layer :as sorted]))

(let [masai  masai/make-temp
      sorted #(sorted/make-temp :layout-fns {:node (-> (constantly [[[:edges :*]]
                                                                    [[]]])
                                                       (sorted/wrap-default-formats)
                                                       (sorted/wrap-revisioned))})]
  (defn make-graph []
    {:masai  (masai)
     :sorted (sorted)}))

(defmacro with-each-layer
  "Execute forms with layer bound to each layer specified or all layers if layers is empty."
  [layers & forms]
  `(doseq [[~'layer-name ~'layer] (as-layer-map ~layers)]
     ~@forms))

(defmacro test-each-layer [layer & forms]
  `(with-each-layer ~layer
     (testing ~'layer-name
       ~@forms)))

(deftest node-info
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (testing "node-ids"
        (assoc-node! layer-name "1" {:foo 0})
        (is (= #{"1"} (set (node-id-seq layer-name))))))))

(deftest update-test
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (testing "update-node! supports arbitrary functions"
        (let [node1 {:foo 2 :bar "three" :baz [1 2 3]}
              node2 {:foo 2 :bar "three"}
              node3 {:foo 2 :bar "three" :baz [5]}
              node4 {:foo 2 :baz [5]}]
          (is (= node2
                 (-> layer-name
                     (txn-> (assoc-node "1" node2)
                            (update-node "1" assoc :baz [1 2 3])
                            (update-node "1" dissoc :baz))
                     (get-node "1"))))
          (is (= node3
                 (-> layer-name
                     (txn-> (update-node "1" assoc :baz [5]))
                     (get-node "1"))))
          (is (= node4
                 (-> layer-name
                     (txn-> (update-node "1" select-keys [:foo :baz]))
                     (get-node "1")))))))))

;; TODO add a way to test that a node isn't gotten multiple times unless writes happen
(deftest caching
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (testing "with-caching"
        (with-caching true
          (do
            (at-revision 100
              (txn-> layer-name
                     (assoc-node "3" {:bar "cat" :baz [5]})
                     (update-node "3" adjoin {:baz [8]})))

            (testing "Write to same node twice ignores cache."
              (at-revision 100
                (is (= {:bar "cat" :baz [5 8]} (get-node layer-name "3")))))

            (at-revision 101
              (txn-> layer-name
                     (update-node "3" adjoin {:baz [9]})))

            (at-revision 101
              (is (= {:bar "cat" :baz [5 8 9]} (get-node layer-name "3"))))))))))

(deftest transactions
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (let [node {:foo 7 :bar "seven"}]
        (with-transaction layer-name
          (assoc-node! layer-name "7" node)
          (is (= node (get-node layer-name "7"))
              "Should see past writes in with-transaction")
          (retro/abort-transaction))
        (is (not (get-node layer-name "7"))
            "Aborted transaction shouldn't apply")
        (is (thrown? Error
                     (with-transaction layer-name
                       (assoc-node! layer-name "7" node)
                       (is (= node (get-node layer-name "7")))
                       (throw (Error.)))))
        (is (not (get-node layer-name "7")))
        (with-transaction layer-name
          (assoc-node! layer-name "7" node))
        (is (= node (get-node layer-name "7")))))))

(deftest properties
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (testing "layer-wide properties"
        (let [keys [:meta :foo]
              curr-foo (fn []
                         (get-in-node layer-name keys))]
          (is (not (curr-foo)))
          (assoc-in-node! layer-name keys [1 2 3])
          (is (= [1 2 3] (curr-foo)))
          (update-in-node! layer-name keys conj 5)
          (is (= [1 2 3 5] (curr-foo))))))))

(deftest incoming
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (testing "keeps track of incoming edges"
        (is (empty? (get-incoming layer-name "1")))
        (assoc-node! layer-name "4" {:edges {"1" {:a "one"}}})
        (is (= #{"4"} (get-incoming layer-name "1")))
        (is (= #{"1"} (set (keys (get-edges layer-name "4")))))
        (assoc-node! layer-name "5" {:edges {"1" {:b "two"}}})
        (is (= #{"4" "5"} (get-incoming layer-name "1")))
        (update-node! layer-name "5" adjoin {:edges {"1" {:deleted true}}})
        (is (= #{"4"} (get-incoming layer-name "1")))
        (update-node! layer-name "4" merge {:edges {"2" {:a "1"} "3" {:b "2"}}})
        (is (= #{"4"} (get-incoming layer-name "2")))
        (is (= #{"4"} (get-incoming layer-name "3")))
        (is (= #{"2" "3"} (set (keys (get-edges layer-name "4")))))))))

(deftest non-transactional-revisions
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)

      (testing "adjoin supports viewing old revisions"
        (let [node  {:bar "cat" :baz [5]}
              attrs {:baz [8]}]
          (at-revision 100
            (assoc-node! layer-name "3" node)
            (is (= node (get-node layer-name "3"))))
          (at-revision 101
            (update-node! layer-name "3" adjoin attrs)
            (is (= (adjoin node attrs) (get-node layer-name "3"))))
          (at-revision 100
            (is (= node (get-node layer-name "3"))))))

      (testing "get-node returns nil if node didn't exist at-revision"
        (at-revision 99
          (is (not (get-node layer-name "3")))))

      (testing "revisions returns an empty list for nodes without revisions"
        (is (empty? (get-revisions layer-name "1"))))

      (testing "revisions are empty when read before they were created"
        (at-revision 1
          (is (empty? (get-revisions layer-name "3")))))

      (testing "max-revision"
        (at-revision 102
          (assoc-node! layer-name "8" {:foo 8}))
        (is (= 8 (:foo (get-node layer-name "8"))))
        (is (= 102 (retro/max-revision layer))))

      (testing "past revisions are ignored inside of dotxn and txn->"
        (at-revision 101
          (txn-> layer-name
                 (assoc-node "8" {:foo 9})))
        (is (= 8 (:foo (get-node layer-name "8"))))))))

(deftest revisioned-incoming
  (with-graph (make-graph)
    (test-each-layer []
      (truncate! layer-name)
      (at-revision 100 (is (empty? (get-incoming layer-name "11"))))
      (at-revision 100
        (txn-> layer-name
               (assoc-node "10" {:edges {"11" {:a "one"}}})))

      (is (= #{"10"} (get-incoming layer-name "11")))
      (is (empty?    (at-revision  99 (get-incoming layer-name "11"))))
      (is (= #{"10"} (at-revision 100 (get-incoming layer-name "11"))))

      (at-revision 200
        (assoc-node! layer-name "12" {:edges {"11" {:a "one"}}}))

      (is (= #{"10" "12"} (get-incoming layer-name "11")))
      (is (= #{"10"}      (at-revision 199 (get-incoming layer-name "11"))))
      (is (= #{"10" "12"} (at-revision 200 (get-incoming layer-name "11"))))

      (at-revision 201
        (txn-> layer-name
               (assoc-node "13" {:edges {"11" {:a "one"}}})))

      (is (= #{"10" "12" "13"}                  (get-incoming layer-name "11")))
      (at-revision 200 (is (= #{"10" "12"}      (get-incoming layer-name "11"))))
      (at-revision 201 (is (= #{"10" "12" "13"} (get-incoming layer-name "11"))))

      (testing "update-in, get-in"
        (at-revision 202
          (with-transaction layer-name
            (update-in-node! layer-name ["13" :edges "11"] adjoin {:a "1"})
            (is (= 202 (uncommitted-revision)))))

        (is (= "1" (get-in-node layer-name ["13" :edges "11" :a])))
        (at-revision 201
          (is (= "one" (get-in-node layer-name ["13" :edges "11" :a])))))

      (testing "node-history"
        (let [history (node-history layer-name "13")]
          (is (sorted? history))
          (is (map? history))
          (is (= (seq history) [[201 {:edges {"11" {:a "one"}}}]
                                [202 {:edges {"11" {:a "1"}}}]])))))))

(deftest test-current-revision
  (with-graph (make-graph)
    (is (zero? (current-revision)))
    (test-each-layer []
      (truncate! layer-name)

      (is (zero? (current-revision layer-name)))

      (testing "assoc-node"
        (at-revision 100 (assoc-node! layer-name "1" {:edges {"2" {:data "whatever"}}}))
        (is (= 100 (current-revision layer-name)))))

    (is (= 100 (current-revision)))

    (test-each-layer []
      (testing "update-in-node"
        (at-revision 200 (update-in-node! layer-name ["1" :memories] (constantly 3)))
        (is (= 200 (current-revision layer-name)))))

    (is (= 200 (current-revision)))

    (test-each-layer []
      (truncate! layer-name)

      (is (zero? (current-revision layer-name))))

    (is (zero? (current-revision)))))

(deftest multi-layer-transactions
  (with-graph (make-graph)
    (letfn [(write [break?]
              (at-revision 100
                (with-transaction :masai
                  (update-in-node! :masai ["x" :edges "y" :times]
                                   conj 1)

                  (with-transaction :sorted
                    (update-in-node! :sorted ["x" :edges "y" :times]
                                     conj 1))

                  (when break?
                    (throw (Exception. "ZOMG"))))))]
      (are [layer] (nil? (get-node layer "x"))
           :masai :sorted)

      (is (thrown? Exception (write true)))
      (is (zero? (current-revision)))
      (are [layer] (nil? (at-revision 0 (get-node layer "x")))
           :masai :sorted)

      (write false)
      (is (= 100 (current-revision)))
      ;; TODO this is known to be breaking; retro needs a redesign before this can work
      (comment
        (are [layer] (= {:edges {"y" {:times [1]}}}
                        (get-node layer "x"))
             :masai :sorted)))))

(deftest null-layer-revisions
  (with-graph (assoc (make-graph)
                :null (null/make))
    (is (= 0 (current-revision) (uncommitted-revision)))

    (at-revision 100
      (with-each-layer []
        (assoc-node! layer-name :foo {:blah 1})))

    (is (= 100 (current-revision) (uncommitted-revision)))

    (with-each-layer [:masai :sorted]
      (is (= {:blah 1} (get-node layer-name :foo))))

    (is (= 432 (get-node :null :foo 432)))))

(comment
  (deftest compact-node
    (with-graph (make-graph)
      (test-each-layer [:tp :tr]
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

  (deftest adhere-schema
    (with-graph
      (into {} (for [[k v] (make-graph)]
                 [k (with-meta v {:types {:foo #{:bar} :bar #{:bar}}})]))
      (test-each-layer []
        (truncate! layer-name)
        (testing "adheres to the schema"
          (is (add-node! layer-name "bar-1" {:a "b"}))
          (is (add-node! layer-name "foo-1" {:edges {"bar-1" {:b "2"}}}))
          (is (thrown-with-msg? AssertionError #"types-valid"
                (add-node! layer-name "baz-1" {:a "b"})))
          (is (thrown-with-msg? AssertionError #"types-valid"
                (update-node! layer-name "baz-1" {:a "b"})))
          (is (thrown-with-msg? AssertionError #"types-valid"
                (append-node! layer-name "baz-1" {:a "b"}))))

        (testing "can find layers with a specific type"
          (is (= [:tr :tp :stm] (layers :foo)))))))

  (deftest test-edges-valid
    (with-graph {:stm1 (stm/make)
                 :stm2 (with-meta (stm/make) {:single-edge true})}
      (doseq [layer (keys *graph*)]
        (truncate! layer))
      (testing "behaves properly when :single-edge is false"
        (is (not (edges-valid? :stm1 {:edge {:id "1"}})))
        (is (edges-valid? :stm1 {:edges {"1" {:a "b"}}})))
      (testing "behaves properly when :single-edge is true"
        (is (edges-valid? :stm2 {:edge {:id "1"}}))
        (is (not (edges-valid? :stm2 {:edges {"1" {:a "b"}}}))))))

  (deftest test-node-valid-node-assert
    (with-graph {:a (with-meta (bal/make (tokyo/make {:path "/tmp/jiraph-test-a" :create true})
                                         (paf/make Test$Node))
                      {:types {:foo #{:baz} :bar #{:baz}} :single-edge true})}
      (doseq [layer (keys *graph*)]
        (truncate! layer))
      (testing "invalid node and edge types"
        (is (not (node-valid? :a "baz-1" {:edge {:id "baz-1"}})))
        (is (not (node-valid? :a "foo-1" {:edge {:id "bar-1"}})))
        (is (thrown-with-msg? AssertionError #"types-valid"
              (verify-node :a "baz-1" {:foo 1}))))
      (testing "multiple edges not allowed"
        (is (not (node-valid? :a "foo-1" {:edges {"baz-8" {:a "1"}}})))
        (is (thrown-with-msg? AssertionError #"edges-valid"
              (verify-node :a "foo-1" {:edges {"baz-8" {:a "1"}}}))))
      (testing "invalid fields"
        (is (not (node-valid? :a "foo-1" {:foo "bar"})))
        (is (not (node-valid? :a "bar-1" {:bar 123})))
        (is (thrown-with-msg? AssertionError #"error setting boolean field Edge.deleted to 1"
              (verify-node :a "foo-1" {:edge {:deleted 1}}))))
      (testing "valid nodes"
        (is (node-valid? :a "foo-1" {:edge {:id "baz-1"} :foo 12 :bar "abc"}))
        (is (node-valid? :a "bar-1" {:edge {:id "baz-1"} :baz 1119}))
        (is (nil? (verify-node :a "foo-1" {:edge {:id "baz-2"} :bar "foo"}))))))

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
                      {:types #{:foo :bam}})
                 :d (with-meta (bal/make (tokyo/make {:path "/tmp/jiraph-test-d" :create true})
                                         (raf/make {:one 1 :two 2 :foo 3}))
                      {:types #{:foo :bar :bam :baz} :hidden true})}
      (is (= {:id    {:type :string},
              :edges {:repeated true, :type :message},
              :edge  {:type :message},
              :rev   {:type :int},
              :baz   {:repeated true, :type :int},
              :bar   {:type :string},
              :bap   {:type :message},
              :foo   {:type :int}}
             (fields :a)))
      (is (= {:id      {:type :string},
              :a       {:type :string},
              :b       {:type :string},
              :deleted {:type :boolean}}
             (fields :a [:edges])))
      (is (= {:id    {:a {:type :string}}
              :edges {:a {:repeated true, :type :message}},
              :edge  {:a {:type :message}},
              :rev   {:a {:type :int}},
              :one   {:c nil},
              :two   {:c nil},
              :foo   {:c nil, :a {:type :int}},
              :bar   {:a {:type :string}},
              :bap   {:a {:type :message}},
              :baz   {:a {:repeated true, :type :int}}}
             (schema :foo)))
      (is (= {:id    {:a {:type :string}},
              :edges {:a {:repeated true, :type :message}},
              :edge  {:a {:type :message}},
              :rev   {:a {:type :int}},
              :bap   {:a {:type :message}, :b {:type :double}},
              :foo   {:b {:type :int}, :a {:type :int}},
              :bar   {:a {:type :string}},
              :baz   {:a {:repeated true, :type :int}}}
             (schema :bar)))
      (is (= {:val {:a {:type :string}},
              :key {:a {:type :int}}}
             (schema :foo :bap)))))

  (deftest single-edge ;; TODO need to update layers to enforce single-edgedness
    (with-graph
      (into {} (for [[k v] (make-graph)] [k (with-meta v {:single-edge true})]))
      (test-each-layer []
        (truncate! layer-name)
        (testing "add-node! and update-node! work with single-edge"
          (is (empty? (get-incoming layer-name "1")))
          (is (add-node! layer-name "4" {:edge {:id "1"}}))
          (is (= #{"4"} (get-incoming layer-name "1")))
          (is (= {"1" {:id "1"}} (get-edges layer-name "4")))
          (is (update-node! layer-name "4" (constantly {:edge {:id "2"}})))
          (is (= #{"4"} (get-incoming layer-name "2")))
          (is (= {"2" {:id "2"}} (get-edges layer-name "4")))
          (is (update-node! layer-name "4" (constantly {:edge {:id "2" :deleted true}})))
          (is (= #{} (get-incoming layer-name "2")))
          (is (= {"2" {:id "2", :deleted true}} (get-edges layer-name "4"))))
        (testing "append-node! and append-edge! work with single-edge"
          (is (empty? (get-incoming layer-name "A")))
          (is (append-node! layer-name "B" {:edge {:id "A"}}))
          (is (= #{"B"} (get-incoming layer-name "A")))
          (is (append-edge! layer-name "C" "A" {}))
          (is (= #{"B" "C"} (get-incoming layer-name "A"))))))))
