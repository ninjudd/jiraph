(ns jiraph.graph-test
  (:use clojure.test jiraph.graph
        [retro.core :as retro :only [dotxn at-revision]])
  (:require [jiraph.stm-layer :as stm]
            [jiraph.layer :as layer]))

(prn "COMPILE GRAPH")

(defn test-layer [master]
  (prn "LAYER TEST")
  (truncate! master)
  (let [rev (vec (for [r (range 5)]
                   (at-revision master r)))
        mike-node {:age 21 :edges {"carla" {:rel :mom}}}]
    (dotxn (rev 1)
      (-> (rev 1)
          (assoc-node "mike" mike-node)
          (assoc-node "carla" {:age 48})))
    (testing "Old revisions are untouched"
      (is (= nil (get-node (rev 0) "mike"))))
    (testing "Node data is written"
      (is (= mike-node (get-node (rev 1) "mike"))))
    ;(prn "=============START=============")
    (testing "Future revisions can be read"
      (is (= mike-node (get-node (rev 4) "mike"))))
    ;(prn "==============END==============")
    (testing "Basic incoming"
      (is (= #{"mike"}
             (get-incoming (rev 1) "carla")))
      (is (empty? (get-incoming (rev 1) "mike"))))

    (dotxn (rev 2)
      (let [actions (-> (rev 2)
                        (assoc-node "charles" {:edges {"carla" {:rel :mom}}})
                        (update-node "charles" assoc :age 18)
                        (update-in-node ["mike" :age] inc))]
        (testing "Writes can't be seen while queueing"
          (is (nil? (get-node actions "charles"))))
        actions))
    (testing "Updates see previous writes"
      (is (= {:age 18 :edges {"carla" {:rel :mom}}}
             (get-node (rev 2) "charles"))))
    (testing "Incoming is revisioned"
      (is (= #{"mike"} (get-incoming (rev 1) "carla")))
      (is (= #{"mike" "charles"} (get-incoming (rev 2) "carla"))))

    (testing "Changelog support"
      (testing "get-revisions"
        (is (= #{1 2} (set (get-revisions (rev 2) "mike"))))
        (testing "Don't know about future revisions"
          (is (= #{1} (set (get-revisions (rev 1) "mike")))))
        (is (= #{1} (set (get-revisions master "carla")))))
      (testing "get-changed-ids"
        (is (= #{"mike" "carla"}
               (set (layer/get-changed-ids master 1))))
        (is (= #{"mike" "charles"}
               (set (layer/get-changed-ids (rev 2) 2)))))
      (testing "max-revision"
        (is (= 2 (layer/max-revision master)))
        (is (= 2 (layer/max-revision (rev 1))))))

    (testing "Can't rewrite history"
      (dotxn (rev 1)
        (-> (rev 1)
            (assoc-node "donald" {:age 72})))
      (doseq [r rev]
        (is (nil? (get-node r "donald")))))

    (testing "Transaction safety"
      (testing "Can't mutate active layer while building a transaction"
        (is (thrown? Exception
                     (dotxn (rev 3)
                       (doto (rev 3)
                         (assoc-node! "stevie" {:age 2}))))))
      (testing "Can't mutate other layer while committing a transaction"
        (is (thrown? Exception
                     (dotxn (rev 3)
                       (-> (rev 3)
                           (retro/enqueue (fn [_]
                                            (assoc-node! (rev 4) "stevie" {:age 2})))))))))

    (testing "Reporting of revision views"
      (is (= 2 (retro/current-revision (rev 2))))
      (is (nil? (retro/current-revision master))))))


(deftest layer-impls
  (doseq [layer [(stm/make)]] ;; add more layers as they're implemented
    (test-layer layer)))
