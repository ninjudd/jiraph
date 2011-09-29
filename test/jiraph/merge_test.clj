(ns jiraph.merge-test
  (:use clojure.test
        jiraph.graph
        [useful.utils :only [into-set]])
  (:require [jiraph.layer :as layer]
            [jiraph.masai-layer :as masai]
            [masai.tokyo :as tokyo])
  (:import [jiraph Test$Node]))

(def graph {:restaurants (masai/make {:db (tokyo/make {:path "/tmp/jiraph-test-tokyo-protobuf-merges-restaurants"
                                                       :create true})})
            :menu-items  (masai/make {:db (tokyo/make {:path "/tmp/jiraph-test-tokyo-protobuf-merges-menu-items"
                                                       :create true})})
            :merge       (masai/make {:db (tokyo/make {:path "/tmp/jiraph-test-tokyo-protobuf-merges-merge"
                                                       :create true})})})
(use-fixtures :each (fn [f]
                      (with-graph graph
                        (truncate!)
                        (f))))

(defn append-edge* [layer-name from-id to-id & [attrs]]
  (append-edge! layer-name from-id to-id (merge {:deleted false} attrs)))

(defn merge! [head-id tail-id]
  (append-edge* :merge tail-id head-id))

(let [node-a    {:name "The Golden State"
                 :address "426 N. Fairfax Ave"}
      node-b    {:name "Golden State"
                 :address "426 N. Fairfax Ave"}
      node-c    {:name "Golden State Cafe"
                 :address "426 N. Fairfax Ave"}
      burger    {:name "The Burger"}
      chocolate {:name "Chocolate Ice Cream"}
      vanilla   {:name "Vanilla Ice Cream"}]

  (deftest test-merge-nodes
    (add-node!    :restaurants "r-1" node-a)
    (add-node!    :menu-items  "m-1" burger)
    (append-edge* :restaurants "r-1" "m-1")

    (add-node!    :restaurants "r-2" node-b)
    (add-node!    :menu-items  "m-2" chocolate)
    (append-edge* :restaurants "r-2" "m-2")

    (add-node!    :restaurants "r-3" node-c)
    (add-node!    :menu-items  "m-3" vanilla)
    (append-edge* :restaurants "r-3" "m-3" {:foo :bar})

    (merge! "m-2" "m-3")
    (merge! "r-1" "r-3")
    (merge! "r-1" "r-2")
    (delete-edge! :restaurants "r-1" "m-2")

    (binding [jiraph.graph/*merge-ids* (fn [id] (let [merge (layer :merge)
                                                      id (or (ffirst (:edges (layer/get-node merge id))) id)]
                                                  (conj (into-set [] (layer/get-incoming merge id)) id)))]
      (is (= #{"m-1"}
             (set (for [[k {:keys [deleted]}] (:edges (get-node :restaurants "r-1"))
                        :when (not deleted)]
                    k)))))))