(ns jiraph.codecs-test
  (:use clojure.test cereal.core jiraph.codecs
        [useful.utils :only [adjoin]])
  (:import (java.nio ByteBuffer)))

(deftest revisioned-codecs
  (doseq [codec [(revisioned (clojure-codec :repeated true) adjoin)
                 (revisioned (java-codec    :repeated true) adjoin)]]
    (testing "append two simple encoded data structures"
      (let [data1 (encode (codec 1) {:foo 1 :bar 2})
            data2 (encode (codec 2) {:foo 4 :baz 8 :bap [1 2 3]})
            data3 (encode (codec 3) {:foo 3 :bap [10 11 12]})
            data  (concat data1 data2 data3)]
        (is (= {:foo 1 :bar 2 :revisions [1]}
               (decode (codec 1) data)))
        (is (= {:foo 4 :bar 2 :baz 8 :bap [1 2 3] :revisions [1 2]}
               (decode (codec 2) data)))
        (is (= {:foo 3 :bar 2 :baz 8 :bap [1 2 3 10 11 12] :revisions [1 2 3]}
               (decode (codec 3) data)))))))