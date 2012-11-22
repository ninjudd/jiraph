(ns flatland.jiraph.layer.cache-test
  (:use clojure.test)
  (:require [flatland.jiraph.layer.cache :as cache]
            [flatland.jiraph.masai-layer :as masai]
            [flatland.jiraph.layer.ruminate :as ruminate]
            [flatland.jiraph.graph :as graph]))

(deftest caching
  (let [a (masai/make-temp)
        b (masai/make-temp)
        c (ruminate/incoming a b)
        layer (cache/make c)
        from-id "profile-1"
        to-id "profile-2"
        existing {:edges {to-id {:deleted false}}}
        deleted {:edges {to-id {:deleted true}}}]
    (graph/open layer)
    (graph/assoc-node! c from-id existing)
    (is (= existing (graph/get-node layer from-id)))
    (is (= #{from-id} (graph/get-incoming layer to-id)))
    (is (thrown? Exception (graph/assoc-node! layer from-id deleted))
        "Should refuse to allow writes on a cached layer")
    (graph/assoc-node! c from-id deleted)
    (is (= existing (graph/get-node layer from-id))
        "Should see stale/cached value")
    (is (= #{from-id} (graph/get-incoming layer to-id)))))
