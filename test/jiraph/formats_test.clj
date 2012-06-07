(ns jiraph.formats-test
  (:use clojure.test jiraph.formats jiraph.formats.cereal retro.core
        [io.core :only [catbytes]]
        [useful.utils :only [adjoin]])
  (:require [jiraph.masai-layer :as masai]
            [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [jiraph.codex :as codex]
            [jiraph.typed :as typing]
            [masai.tokyo :as tokyo]
            [ego.core :as ego]
            [jiraph.formats.protobuf :as proto])
  (:import (java.nio ByteBuffer)
           (jiraph Test$Foo)))

(deftest revisioned-codecs
  (doseq [impl [revisioned-clojure-format revisioned-java-format]
          :let [format-fn (impl adjoin)]]
    (letfn [(encode [node revision]
              (codex/encode (:codec (format-fn {:revision revision})) node))
            (decode [bytes revision]
              (codex/decode (:codec (format-fn {:revision revision})) bytes))]
     (testing "append two simple encoded data structures"
       (let [data1 (encode {:foo 1 :bar 2}              1)
             data2 (encode {:foo 4 :baz 8 :bap [1 2 3]} 2)
             data3 (encode {:foo 3 :bap [10 11 12]}     3)
             data  (catbytes data1 data2 data3)]
         (doseq [[rev expect] [[1 {:foo 1 :bar 2}]
                               [2 {:foo 4 :bar 2 :baz 8 :bap [1 2 3]}]
                               [3 {:foo 3 :bar 2 :baz 8 :bap [1 2 3 10 11 12]}]]]
           (let [node (decode data rev)]
             (is (= (-> node meta :revisions last) rev))
             (is (= node expect)))))))))

(deftest typed-layers
  (let [base (revisioned-clojure-format adjoin)
        id "person-1"]
    (masai/with-temp-layer [base-layer :format-fns {:node base}]
      (let [l (at-revision base-layer 1)]
        (dotxn l
          (-> l
              (graph/assoc-node id {:foo :blah})))
        (is (= {:foo :blah}
               (graph/get-node l id)))
        (is (= [1] (graph/get-revisions l id))))
      (let [wrapped-layer (typing/typed-layer base-layer {:profile #{:photo :biography}})
            rev (memoize (fn [rev]
                           (at-revision wrapped-layer rev)))]
        (is (thrown? Exception ;; refuses to write "person"s
                     (graph/assoc-node! (rev 2) id {:foo :blah})))

        (let [l (rev 2)
              id "profile-2"]
          (graph/assoc-node! l id {:foo :blah})
          (is (= {:foo :blah} (graph/get-node l id)))
          (is (= [2] (graph/get-revisions l id))))

        (let [l (rev 3)
              id "profile-8"
              bad-data {:edges {"whatever-10" {:attr :value}}}]
          (are [keyseq] (thrown? Exception
                                 (graph/update-in-node! l (cons id keyseq) adjoin
                                                        (get-in bad-data keyseq)))
               []
               [:edges]
               [:edges "whatever-10"]
               [:edges "whatever-10" :attr])
          (let [data {:foo :bar, :edges {"photo-1" {:location "whatever"}}}]
            (graph/assoc-node! l id data)
            (is (= data (graph/get-node l id)))
            (is (= [3] (graph/get-revisions l id)))))

        (testing "Should work with functional interface"
          (let [l (rev 4)]
            (is (thrown? Exception
                         (dotxn l
                           (-> l
                               (graph/assoc-node "person-4" {:foo :bar})))))
            (let [id "profile-7"]
              (dotxn l
                (-> l
                    (graph/assoc-node id {:foo :bar})))
              (is (= {:foo :bar} (graph/get-node l id))))))))))

(deftest protobuf-sets
  (let [real-adjoin adjoin
        throwing-adjoin (fn [a b]
                          (throw (Exception. (format "Attempted to adjoin %s onto %s."
                                                     (pr-str b) (pr-str a)))))]
    (with-redefs [adjoin throwing-adjoin]
      (let [master (masai/make (tokyo/make {:path "/tmp/jiraph-cached-walk-test-foo" :create true})
                               :format-fns {:node (proto/protobuf-format Test$Foo)})
            rev (vec (for [r (range 5)]
                       (at-revision master r)))
            before {:bar 5, :tag-set #{"a" "b"}}
            change {:bar 7, :tag-set {"c" true "b" false}}
            after  {:bar 7, :tag-set #{"a" "c"}}]

        (layer/open master)
        (layer/truncate! master)

        (is (= after (real-adjoin before change)))

        (dotxn (rev 1) ;; adjoin function should be optimized away for reads and writes
          (-> (rev 1)
              (graph/update-node "1" adjoin before)))
        (is (= before
               (graph/get-node (rev 1) "1")))

        ;; here the function can't be optimized, so we should read, adjoin, write
        (is (thrown? Exception
                     (dotxn (rev 2)
                       (-> (rev 2) (graph/update-node "1" (comp identity adjoin) change)))))

        (dotxn (rev 2) ;; now make the change for real, without the breaking adjoin
          (-> (rev 2) (graph/update-node "1" adjoin change)))

        (is (= after
               (graph/get-node (rev 2) "1")))

        (layer/close master)))))
