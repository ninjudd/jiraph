(ns flatland.jiraph.phantom-merge-test
  (:use flatland.jiraph.merge
        clojure.test)
  (:require [flatland.jiraph.layer :as layer]))

(comment

  (deftest test-M
    (is (= (M {:foo 1 :edges {:a {:exists true :val 1}
                              :b {:val 2}}}
              {:blah 3 :foo 8 :edges {:a {:exists true :x 20}
                                      :b {:exists true :val 9}}})
           {:foo 1, :blah 3, :edges {:b {:exists true, :val 9},
                                     :a {:val 1, :exists true, :x 20}}}))
    (is (= (M {:foo 1 :edges {:a {:exists true :val 1}
                              :b {:val 2}}}
              {:blah 3 :foo 8 :edges {:a {:exists true :x 20 :val :q}
                                      :b {:exists true :val 9}}})
           {:foo 1, :blah 3, :edges {:b {:exists true, :val 9},
                                     :a {:exists true, :x 20, :val 1}}}))
    (is (= (M {:foo 1 :edges {:a {:exists true :val 1}
                              :b {:val 2}}}
              {:blah 3 :foo 8 :edges {:a {:exists true :x 20 :val :q}
                                      :b {:val 9}}})
           {:foo 1, :blah 3, :edges {:a {:exists true, :x 20, :val 1}}})))

  (deftest test-E
    (is (= (E {:edges {:a {:foo 1 :bar 3}
                       :b {:foo 2}}}
              {:b [:c {:position 0}]
               :a [:c {:position 1}]}
              {:c :a})
           {:edges {:a {:foo 1, :bar 3}, :b {:foo 2}}})))

  (deftest test-edge-reading
    ;; merge:       R1       R2
    ;;             /  \     /  \
    ;;            A*   B   C*   D
    ;;
    ;; links:    A -> C {:foo true}
    ;;           B -> D {:foo false}
    (let [merge-layer (reify
                        layer/Parent
                        (child [this k]
                          ({:incoming :incoming} k))
                        Object
                        (toString [this]
                          "merge"))
          links-layer (reify
                        clojure.lang.ILookup
                        (valAt [this k]
                          ({:merge-layer merge-layer
                            :cache-layer :cache
                            :layer :links} k))
                        Object
                        (toString [this]
                          "links"))
          layer-data {merge-layer '{R1 {:head A :edges {A {:exists true}, B {:exists true}}}
                                    A {:edges {R1 {:exists true :position 0}}}
                                    B {:edges {R1 {:exists true :position 1}}}
                                    R2 {:head C :edges {C {:exists true}, D {:exists true}}}
                                    C {:edges {R2 {:exists true :position 0}}}
                                    D {:edges {R2 {:exists true :position 1}}}}
                      :incoming '{A {:edges {R1 {:exists true}}}
                                  B {:edges {R1 {:exists true}}}
                                  R1 {:edges {A {:exists true :position 0}
                                              B {:exists true :position 1}}}
                                  C {:edges {R2 {:exists true}}}
                                  D {:edges {R2 {:exists true}}}
                                  R2 {:edges {C {:exists true :position 0}
                                              D {:exists true :position 1}}}}
                      :links '{A {:edges {C {:exists true :foo true}}}
                               B {:edges {D {:exists true :foo false}}}}
                      :cache '{R1 {:edges {C {:exists true :foo true}
                                           D {:exists true :foo false}}}}}
          read (fn [layer keys & [not-found]]
                 (get-in layer-data (cons layer keys) not-found))
          annotated-read (fn [layer keys & [not-found]]
                           (doto (read layer keys not-found)
                             (->> (prn (cons (symbol (str layer)) keys) '=> ,,,))))
          ;; read annotated-read
          ]
      (is (= '[C {:exists true :foo true}]
             (read-one-edge read links-layer 'A 'C)
             (read-one-edge read links-layer 'B 'C)
             (read-one-edge read links-layer 'A 'D)
             (read-one-edge read links-layer 'B 'D)))
      (is (= '{:edges {C {:exists true :foo true}}}
             (read-node read links-layer '[A])
             (read-node read links-layer '[B])))
      (is (= '{C {:exists true :foo true}}
             (read-node read links-layer '[A :edges])
             (read-node read links-layer '[B :edges])))
      (is (= '{:exists true :foo true}
             (read-node read links-layer '[A :edges C])
             (read-node read links-layer '[B :edges C])
             (read-node read links-layer '[A :edges D])
             (read-node read links-layer '[B :edges D])))
      (is (= 'arglebargle
             (read-node read links-layer '[A :foo] 'arglebargle)))
      (let [read (fn [layer keyseq & [not-found]]
                   (-> layer-data
                       (assoc-in '[:links R1 :edges C] {:exists false})
                       (get-in (cons layer keyseq) not-found)))]
        (is (= nil
               (read-one-edge read links-layer 'A 'C)
               (read-one-edge read links-layer 'B 'C)
               (read-one-edge read links-layer 'A 'D)
               (read-one-edge read links-layer 'B 'D)))))))
