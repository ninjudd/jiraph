(ns jiraph.layer-test
  (:refer-clojure :exclude [sync count])
  (:use clojure.test jiraph.layer)
  (:require [jiraph.byte-append-layer :as bal]
            [jiraph.tokyo-database :as tokyo]
            [jiraph.reader-append-format :as raf]))

(deftest layer
  (let [db    (tokyo/make {:path "/tmp/jiraph-test-tokyo-layer" :create true})
        layer (bal/make db (raf/make))]
    (open layer)
    (truncate! layer)

    (testing "add-node! won't overwrite existing node"
      (let [node {:foo 2 :bar 3}]
        (is (= node (add-node! layer 1 node)))
        (is (= node (get-node layer 1)))
        (is (= nil  (add-node! layer 1 {:foo 8})))
        (is (= node (get-node layer 1)))))

    (testing "assoc-node! modifies specific attributes"
      (let [node {:foo 54 :bar 3 :baz [1 2 3]}]
        (is (= node (assoc-node! layer 1 [:foo 54 :baz [1 2 3]])))
        (is (= node (get-node layer 1)))))

    (testing "assoc-node! creates node if it doesn't exist"
      (let [node {:foo 9 :bar 43}]
        (is (= node (assoc-node! layer 2 [:foo 9 :bar 43])))
        (is (= node (get-node layer 2)))))

    (testing "count and node-exists?"
      (is (= 2 (count layer)))
      (doseq [id [1 2]]
        (is (node-exists? layer id)))
      (doseq [id [8 9 234]]
        (is (not (node-exists? layer id)))))

    (testing "update-node! supports artitrary functions"
      (let [node {:foo 54 :bar 3}]
        (is (= node (update-node! layer 1 dissoc [:baz])))
        (is (= node (get-node layer 1))))
      (let [node {:foo 54 :bar 3 :baz 5}]
        (is (= node (update-node! layer 1 assoc [:baz 5])))
        (is (= node (get-node layer 1))))
      (let [node {:foo 54 :baz 5}]
        (is (= node (update-node! layer 1 select-keys [[:foo :baz]])))
        (is (= node (get-node layer 1)))))

    (testing "append-node! supports viewing old revisions"
      (let [node  {:bar 3 :baz 5 :rev 100}
            attrs {:baz 8 :rev 101}]
        (binding [*rev* 100]
          (is (= node (append-node! layer 3 (dissoc node :rev))))
          (is (= node (get-node layer 3))))
        (binding [*rev* 101]
          (is (= attrs              (append-node! layer 3 (dissoc attrs :rev))))
          (is (= (merge node attrs) (get-node layer 3))))
        (binding [*rev* 100]
          (is (= node (get-node layer 3))))))

    (testing "compact-node! removes revisions but leaves all-revisions"
      (is (= '(100 101) (revisions layer 3)))
      (is (= '(100 101) (all-revisions layer 3)))
      (is (= {:bar 3, :baz 8} (compact-node! layer 3)))
      (is (= () (revisions layer 3)))
      (is (= '(100, 101) (all-revisions layer 3))))

    (testing "revisions and all-revisions returns an empty list for nodes without revisions"
      (is (= () (revisions layer 1)))
      (is (= () (all-revisions layer 1))))

    (testing "incoming keeps track of incoming edges"
      (is (= #{} (incoming layer 1)))
      (is (add-node! layer 4 {:edges {1 {:foo "one"}}}))
      (is (= #{4} (incoming layer 1)))
      (is (add-node! layer 5 {:edges {1 {:bar "two"}}}))
      (is (= #{4 5} (incoming layer 1)))
      (is (append-node! layer 5 {:edges {1 {:deleted true}}}))
      (is (= #{4} (incoming layer 1)))
      (is (update-node! layer 4 #(assoc % :edges {2 {:foo 1} 3 {:bar 2}}) []))
      (is (= #{}  (incoming layer 1)))
      (is (= #{4} (incoming layer 2)))
      (is (= #{4} (incoming layer 3))))

    (close layer)))