(ns jiraph.ruminate-test
  (:use clojure.test jiraph.layer.ruminate useful.utils)
  (:require [jiraph.layer :as layer]
            [jiraph.graph :as graph]
            [jiraph.masai-layer :as masai]))

(deftest simple-indexing
  (let [[src ids ages] (repeatedly masai/make-temp)
        ruminant (make src [ids ages]
                       (fn [{:keys [old new], [id & keys] :keyseq}]
                         (graph/update-node! ids id adjoin {:present true})
                         (when-let [get-age (case (seq keys)
                                              nil :age
                                              [:age] identity
                                              nil)]
                           (when-let [age (get-age new)]
                             (graph/update-in-node! ages [(str age) :count] (fnil inc 0)))
                           (when-let [old-age (get-age old)]
                             (graph/update-in-node! ages [(str old-age) :count] (fnil dec 0))))))
        layers [ruminant ids ages]
        test-data '[[x 10] [y 10] [z 5]]]
    (doseq [layer layers]
      (layer/open layer))
    (doseq [[id age] test-data]
      (graph/assoc-node! ruminant id {:age age}))
    (is (= {:count 2} (graph/get-node ages "10")))
    (is (= {:count 1} (graph/get-node ages "5")))

    (graph/assoc-in-node! ruminant ["x" :age] 5)
    (is (= {:count 1} (graph/get-node ages "10")))
    (is (= {:count 2} (graph/get-node ages "5")))

    (is (= {:present true} (graph/get-node ids "x")))))