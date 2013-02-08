(ns flatland.jiraph.ruminate-test
  (:use clojure.test flatland.jiraph.graph)
  (:require [flatland.jiraph.layer.masai :as masai]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.retro.core :refer [at-revision]]
            [flatland.useful.utils :refer [adjoin]]))

(deftest indexing-works
  (let [base (masai/make-temp)
        index (masai/make-temp)
        indexed (ruminate/top-level-indexer base index :name :named-this)]
    (open indexed)
    (txn (update-node indexed "profile-1" adjoin {:name "steve"}))
    (is (= "steve" (get-in-node base ["profile-1" :name])))
    (is (= {"profile-1" true} (get-in-node index ["steve" :named-this])))))

(deftest changelog-works
  (let [base (masai/make-temp)
        changelog (masai/make-temp)
        tracking (ruminate/changelog base changelog)]
    (open tracking)
    (txn (compose (assoc-node (at-revision tracking 0) "profile-1" {:name "steve"})
                  (update-in-node (at-revision tracking 0)
                                  ["profile-2" :foo] adjoin {:bar "sdfa"})))
    (is (= "steve" (get-in-node base ["profile-1" :name])))
    (is (= {:bar "sdfa"} (get-in-node base ["profile-2" :foo])))
    (is (= #{"profile-1" "profile-2"} (set (get-in-node changelog ["revision-1" :ids]))))))
