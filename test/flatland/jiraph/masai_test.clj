(ns flatland.jiraph.masai-test
  (:use clojure.test flatland.jiraph.graph)
  (:require [gloss.core :as gloss]
            [flatland.jiraph.masai-layer :as masai]
            [flatland.jiraph.layer :as layer]))

(deftest id-based-formats
  (let [profile-stub {:type :profile}
        other-stub {:type :other}
        format-builder (fn [{:keys [id]}]
                        {:codec
                         (if (.startsWith id "profile")
                           ;; profiles get written as 100 and read as :profile, regardless of data
                           (gloss/compile-frame :int32 (constantly 100) (constantly profile-stub))
                           ;; others are written as 500 and read as :other
                           (gloss/compile-frame :int32 (constantly 500) (constantly other-stub)))})]
    (masai/with-temp-layer [layer :format-fn format-builder]
      (assoc-node! layer "profile-1" {:data :whatever})
      (assoc-node! layer "user-22" {:this :is-ignored})
      (is (= profile-stub (get-node layer "profile-1")))
      (is (= other-stub (get-node layer "user-22"))))))
