(ns jiraph.byte-append-format-test
  (:refer-clojure :exclude [load])
  (:use clojure.test jiraph.byte-append-format)
  (:require [jiraph.reader-append-format   :as raf]
            [jiraph.protobuf-append-format :as paf])
  (:import [jiraph Test$Foo]))

(defn catbytes [& args]
  (.getBytes (apply str (map #(String. %) args))))

(deftest byte-append-format
  (doseq [format [(raf/make) (paf/make Test$Foo)]]
    (testing "load a dumped data structure"
      (let [val {:foo 1 :bar 2}]
        (is (= val (load format (dump format val))))))
    
    (testing "append two simple dumped data structures"
      (let [data1 (dump format {:foo 1 :bar 2})
            data2 (dump format {:foo 4 :baz 8})]
        (is (= {:foo 4 :bar 2 :baz 8}
               (load format (catbytes data1 data2))))))

    (testing "concat lists when appending"
      (let [data1 (dump format {:tags ["foo" "bar"]  :foo 1})
            data2 (dump format {:tags ["baz" "foo"] :foo 2})]
        (is (= {:foo 2 :tags ["foo" "bar" "baz" "foo"]}
               (load format (catbytes data1 data2))))))

    (testing "merge maps when appending"
      (let [data1 (dump format {:num-map {1 "one" 3 "three"}})
            data2 (dump format {:num-map {2 "dos" 3 "tres"}})
            data3 (dump format {:num-map {3 "san" 6 "roku"}})]
        (is (= {:num-map {1 "one" 2 "dos" 3 "san" 6 "roku"}}
               (load format (catbytes data1 data2 data3))))))

    (testing "merge sets when appending"
      (let [data1 (dump format {:tag-set #{"foo" "bar"}})
            data2 (dump format {:tag-set #{"baz" "foo"}})]
        (is (= {:tag-set #{"foo" "bar" "baz"}}
               (load format (catbytes data1 data2))))))

    (testing "support set deletion using existence map"
      (let [data1 (dump format {:tag-set #{"foo" "bar" "baz"}})
            data2 (dump format {:tag-set {"baz" false "foo" true "zap" true "bam" false}})]
        (is (= {:tag-set #{"foo" "bar" "zap"}}
               (load format (catbytes data1 data2))))))
    
    (testing "merge and append nested data structures when appending"
      (let [data1 (dump format {:nested {:foo 1 :tags ["bar"] :nested {:tag-set #{"a" "c"}}}})
            data2 (dump format {:nested {:foo 4 :tags ["baz"] :bar 3}})
            data3 (dump format {:nested {:baz 5 :tags ["foo"] :nested {:tag-set {"b" true "c" false}}}})]
        (is (= {:nested {:foo 4 :bar 3 :baz 5 :tags ["bar" "baz" "foo"] :nested {:tag-set #{"a" "b"}}}}
               (load format (catbytes data1 data2 data3)))))
      )
    
    ))
