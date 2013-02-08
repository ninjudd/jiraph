(ns flatland.jiraph.cache-test
  (:use clojure.test)
  (:require [flatland.jiraph.cache :as cache]
            [flatland.jiraph.layer.masai :as masai]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.graph :as graph]))

(deftest caching
  (let [uncached (ruminate/incoming (masai/make-temp) (masai/make-temp))
        cached (cache/make uncached)
        from-id "profile-1"
        to-id "profile-2"
        existing {:edges {to-id {:exists true}}}
        deleted {:edges {to-id {:exists false}}}]
    (graph/open cached)
    (graph/assoc-node! uncached from-id existing)
    (is (= existing (graph/get-node cached from-id)))
    (is (= #{from-id} (graph/get-incoming cached to-id)))
    (is (thrown? Exception (graph/assoc-node! cached from-id deleted))
        "Should refuse to allow writes on a cached layer")
    (graph/assoc-node! uncached from-id deleted)
    (is (= existing (graph/get-node cached from-id))
        "Should see stale/cached value")
    (is (= #{from-id} (graph/get-incoming cached to-id)))))
