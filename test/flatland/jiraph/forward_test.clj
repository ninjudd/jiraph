(ns flatland.jiraph.forward-test
  (:use clojure.test flatland.jiraph.graph
        [flatland.retro.core :as retro :only [at-revision]])
  (:require [flatland.jiraph.layer :as layer]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.forward :as forward]
            [flatland.jiraph.layer.masai-sorted :as sorted]))

(deftest test-forwarding
  (let [layer (-> (sorted/make-temp)
                  (ruminate/incoming (sorted/make-temp)
                                     #(select-keys % [:exists :data]))
                  (ruminate/top-level-indexer (sorted/make-temp) :index-this, :ids)
                  (forward/make (constantly #(.toUpperCase %))
                                #{:incoming}))]
    (open layer)
    (txn (compose (assoc-node layer "alex" {:edges {"SAsHA" {:data 10}}
                                            :index-this "sample"})
                  (update-in-node layer ["ALEX" :edges "sasha" :data] inc)))
    (is (= {:edges {"SASHA" {:data 11}}, :index-this "sample"}
           (get-node layer "alex")))
    (is (= {:data 11} (get-in-node layer ["aLEx" :edges "SasHA"])))
    (is (= {:edges {"ALEX" {:data 11}}}
           (get-node (layer/child layer :incoming)
                     "SAShA")))
    (is (= {"ALEX" true} (get-in-node (layer/child layer :index-this)
                                      ["sample" :ids])))
    (close layer)))
