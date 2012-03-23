(ns jiraph.codecs-test
  (:use clojure.test jiraph.codecs jiraph.codecs.cereal retro.core
        [useful.utils :only [adjoin]])
  (:require [jiraph.masai-layer :as masai]
            [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [masai.tokyo :as tokyo]
            [jiraph.codecs.protobuf :as proto])
  (:import (java.nio ByteBuffer)
           (jiraph Test$Foo)))

(deftest revisioned-codecs
  (doseq [impl [revisioned-clojure-codec revisioned-java-codec] ; (a -> a) -> (opts -> Codec)
          :let [codec (impl adjoin)]]     ; (opts -> Codec)
    (testing "append two simple encoded data structures"
      (let [data1 (encode codec {:foo 1 :bar 2}              {:revision 1})
            data2 (encode codec {:foo 4 :baz 8 :bap [1 2 3]} {:revision 2})
            data3 (encode codec {:foo 3 :bap [10 11 12]}     {:revision 3})
            data  (concat data1 data2 data3)]
        (doseq [[rev expect] [[1 {:foo 1 :bar 2}]
                              [2 {:foo 4 :bar 2 :baz 8 :bap [1 2 3]}]
                              [3 {:foo 3 :bar 2 :baz 8 :bap [1 2 3 10 11 12]}]]]
          (let [node (decode codec data {:revision rev})]
            (is (= (-> node meta :revisions last) rev))
            (is (= node expect))))))))

(deftest typed-layers
  (let [base (revisioned-clojure-codec adjoin)
        wrapped (wrap-typing base {:profile :union})
        id "person-1"]
    (masai/with-temp-layer [base-layer :formats {:node base}]
      (let [l (at-revision base-layer 1)]
        (dotxn l
          (-> l
              (graph/assoc-node id {:foo :blah})))
        (is (= {:foo :blah}
               (graph/get-node l id)))
        (is (= [1] (graph/get-revisions l id)))))
    (masai/with-temp-layer [wrapped-layer :formats {:node wrapped}]
      (let [l (at-revision wrapped-layer 1)]
        (is (thrown? Exception ;; due to no codec for writing "person"s
                     (dotxn l
                       (-> l
                           (graph/assoc-node id {:foo :blah})))))
        (let [id "profile-1"]
          (dotxn l
            (-> l
                (graph/assoc-node id {:foo :blah})))
          (is (= {:foo :blah}
                 (graph/get-node l id)))
          (is (= [1] (graph/get-revisions l id))))))))

(deftest protobuf-sets
  (let [master (masai/make (tokyo/make {:path "/tmp/jiraph-cached-walk-test-foo" :create true})
                           :formats {:node (proto/protobuf-codec Test$Foo)})
        rev (vec (for [r (range 5)]
                   (at-revision master r)))
        before {:bar 5, :tag-set #{"a" "b"}}
        change {:bar 7, :tag-set {"c" true "b" false}}
        after  {:bar 7, :tag-set #{"a" "c"}}]

    (layer/open master)
    (layer/truncate! master)

    (is (= after (adjoin before change)))

    (dotxn (rev 1)
      (-> (rev 1)
          (graph/assoc-node "1" before)))
    (is (= before
           (graph/get-node (rev 1) "1")))

    (dotxn (rev 2)
      (-> (rev 2) (graph/update-node "1" (comp identity adjoin) change)))
    (is (= after
           (graph/get-node (rev 2) "1")))

    (layer/close master)))
