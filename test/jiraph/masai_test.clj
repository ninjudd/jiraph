(ns jiraph.masai-test
  (:use clojure.test jiraph.graph)
  (:require [gloss.core :as gloss]
            [jiraph.masai-layer :as masai]
            [jiraph.layer :as layer]))

(deftest id-based-codecs
  (let [profile-stub {:type :profile}
        other-stub {:type :other}
        codec-builder (fn [{:keys [id]}]
                        (if (.startsWith id "profile")
                          ;; profiles get written as 100 and read as :profile, regardless of data
                          (gloss/compile-frame :int32 (constantly 100) (constantly profile-stub))
                          ;; others are written as 500 and read as :other
                          (gloss/compile-frame :int32 (constantly 500) (constantly other-stub))))]
    (masai/with-temp-layer [layer :formats {:node codec-builder}]
      (assoc-node! layer "profile-1" {:data :whatever})
      (assoc-node! layer "user-22" {:this :is-ignored})
      (is (= profile-stub (get-node layer "profile-1")))
      (is (= other-stub (get-node layer "user-22"))))))
