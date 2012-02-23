(ns jiraph.codecs-test
  (:use clojure.test jiraph.codecs jiraph.codecs.cereal
        [useful.utils :only [adjoin]])
  (:import (java.nio ByteBuffer)))

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
