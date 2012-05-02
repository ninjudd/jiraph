(ns jiraph.formats-test
  (:use clojure.test jiraph.formats jiraph.formats.cereal retro.core
        [useful.utils :only [adjoin]])
  (:require [jiraph.masai-layer :as masai]
            [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [masai.tokyo :as tokyo]
            [gloss.io :as gloss]
            [ego.core :as ego]
            [jiraph.formats.protobuf :as proto])
  (:import (java.nio ByteBuffer)
           (jiraph Test$Foo)))

(deftest revisioned-codecs
  (doseq [impl [revisioned-clojure-format revisioned-java-format]
          :let [format-fn (impl adjoin)]]
    (letfn [(encode [node revision]
              (gloss/encode (:codec (format-fn {:revision revision})) node))
            (decode [bytes revision]
              (gloss/decode (:codec (format-fn {:revision revision})) bytes))]
     (testing "append two simple encoded data structures"
       (let [data1 (encode {:foo 1 :bar 2}              1)
             data2 (encode {:foo 4 :baz 8 :bap [1 2 3]} 2)
             data3 (encode {:foo 3 :bap [10 11 12]}     3)
             data  (concat data1 data2 data3)]
         (doseq [[rev expect] [[1 {:foo 1 :bar 2}]
                               [2 {:foo 4 :bar 2 :baz 8 :bap [1 2 3]}]
                               [3 {:foo 3 :bar 2 :baz 8 :bap [1 2 3 10 11 12]}]]]
           (let [node (decode data rev)]
             (is (= (-> node meta :revisions last) rev))
             (is (= node expect)))))))))

(deftest typed-layers
  (let [base (revisioned-clojure-format adjoin)
        wrapped (wrap-typing base (comp #{:profile} ego/type-key))
        id "person-1"]
    (masai/with-temp-layer [base-layer :format-fns {:node base}]
      (let [l (at-revision base-layer 1)]
        (dotxn l
          (-> l
              (graph/assoc-node id {:foo :blah})))
        (is (= {:foo :blah}
               (graph/get-node l id)))
        (is (= [1] (graph/get-revisions l id)))))
    (masai/with-temp-layer [wrapped-layer :format-fns {:node wrapped}]
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
                           :format-fns {:node (proto/protobuf-format Test$Foo)})
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
