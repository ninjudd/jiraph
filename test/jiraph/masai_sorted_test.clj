(ns jiraph.masai-sorted-test
  (:use clojure.test jiraph.graph
        [useful.utils :only [adjoin]])
  (:require [gloss.core :as gloss]
            [jiraph.masai-sorted-layer :as masai]
            [jiraph.layer :as layer]))

(deftest stuff-works
  ;; these paths are all writing with the default codec (cereal's revisioned clojure-reader)
  (masai/with-temp-layer [layer :formats {:node [[[:edges :*]]
                                                 [[:names]] ;; TODO support indexing non-maps
                                                 [[]]]}]
    (let [init-node {:edges {"profile-10" {:rel :child}}
                     :age 24, :names {:first "Clancy"}}]
      (is (update-in-node! layer ["profile-1"] adjoin init-node))
      (is (= init-node (get-node layer "profile-1")))
      (is (update-in-node! layer ["profile-1" :edges] assoc "profile-21" {:bond :strong}))
      (is (= (assoc-in init-node [:edges "profile-21"] {:bond :strong})
             (get-node layer "profile-1"))))))
