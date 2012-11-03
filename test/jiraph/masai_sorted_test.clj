(ns jiraph.masai-sorted-test
  (:use clojure.test jiraph.graph
        [useful.utils :only [adjoin]])
  (:require [gloss.core :as gloss]
            [jiraph.masai-sorted-layer :as masai]
            [jiraph.layer :as layer]
            [clojure.walk :as walk]))

(defn =*
  "Compare for equality, but treating nil the same as an empty collection."
  [& args]
  (let [nil? #(or (nil? %) (= % {}))]
    (apply = (for [x args]
               (walk/postwalk (fn [x]
                                (cond (nil? x) nil
                                      (map? x) (into (empty x)
                                                     (for [[k v] x
                                                           :when (not (nil? v))]
                                                       [k v]))
                                      :else x))
                              x)))))

(deftest stuff-works
  ;; these paths are all writing with the default codec:
  ;; cereal's revisioned clojure-reader codec
  (let [= =*] ;; jiraph treats {} and nil equivalently; test must account for this
    (masai/with-temp-layer [layer
                            :layout-fn (-> (constantly [{:pattern [:edges :*]}
                                                        {:pattern [:names]}
                                                        {:pattern []}])
                                           (masai/wrap-default-formats)
                                           (masai/wrap-revisioned))]
      (let [id "profile-1"
            init-node {:edges {"profile-10" {:rel :child}}
                       :age 24, :names {:first "Clancy"}}
            node-2 (assoc-in init-node [:edges "profile-21"] {:bond :strong})
            node-3 (update-in node-2 [:names] dissoc :first)

            change-4 {:names {:first "Clancy"}
                      :edges {"profile-10" {:rel :partner}}}
            node-4 (adjoin node-3 change-4)]

        (update-in-node! layer [id] adjoin init-node)
        (is (= init-node (get-node layer id)))

        (update-in-node! layer [id :edges] assoc "profile-21" {:bond :strong})
        (is (= node-2 (get-node layer id)))

        (update-in-node! layer [id :names] dissoc :first)
        (is (= node-3 (get-node layer id)))

        (update-in-node! layer [id] adjoin change-4)
        (is (= node-4 (get-node layer id)))))))

(deftest test-subseq
  (masai/with-temp-layer [layer
                          :layout-fn (-> (constantly [{:pattern [:edges :*]}
                                                      {:pattern []}])
                                         (masai/wrap-default-formats)
                                         (masai/wrap-revisioned))]
    (let [node {:edges {"mary" {:data 1}
                        "charlie" {:data 2}
                        "sally" {:data 3}}}
          sorted (into (sorted-map) (:edges node))]

      ;; make sure there are nodes "around" him to test subseq boundaries
      (assoc-node! layer "charlie" {:data "test"})
      (assoc-node! layer "mike" node)
      (assoc-node! layer "sally" {:data "test"})

      (is (= node (get-node layer "mike")))

      (doseq [[f & args] [[seq]
                          [rseq]
                          [subseq > "mary"]
                          [subseq <= "mary"]
                          [subseq >= "mary" < "sally"]
                          [rsubseq > "david"]
                          [rsubseq <= "david"]
                          [rsubseq > "alex" < "zebediah"]]]
        (is (= (seq (apply f sorted args))
               (seq (apply query-in-node layer ["mike" :edges] f args)))))

      (is (= ["charlie" "mike" "sally" (layer/node-id-seq layer)]))
      ;; (is (= ["mike" "sally" (layer/node-id-subseq layer > "edward")]))
      ;; (is (= [["sally" {:data "test"}]] (layer/node-subseq layer > "nicholas")))
      )))
