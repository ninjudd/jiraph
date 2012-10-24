(ns jiraph.ruminate-test
  (:use clojure.test jiraph.graph)
  (:require [jiraph.masai-layer :as masai]
            [jiraph.layer.ruminate :as ruminate]
            [useful.utils :refer [adjoin]]))

(deftest indexing-works
  (let [base (masai/make-temp)
        index (masai/make-temp)
        indexed (ruminate/top-level-indexer base index :name :named-this)]
    (open indexed)
    (txn (update-node indexed "profile-1" adjoin {:name "steve"}))
    (is (= "steve" (get-in-node base ["profile-1" :name])))
    (is (= {"profile-1" true} (get-in-node index ["steve" :named-this])))))
