(defn- ruminate-merge [merge-layer base-layers keyseq f args]
  (fn [read]
    ))

(defn- ruminate-merging [base-layer [phantom-layer merge-layer] keyseq f args]
  (fn [read]
    (-> (if-let [head (merge-head read merge-layer (get-id keyseq))]
          (let [keyseq (update keyseq for head-id)]
            (apply compose (for [layer [base-layer phantom-layer]]
                             (update-in-node layer keyseq f args))))
          (update-in-node base-layer keyseq f args))
        (invoke read))))

(defn merged
  "layers needs to be a map of layer names to base layers. The base layer will be used to store a
   merged view of tha data written to the merging layer, as determined by merges written to the
   merge-layer. Each base layer must have a child named :phantom, which will be used to store
   internal bookkeeping data, and should not be used by client code.

   Will return a list, [new-merge-layer {layer-name1 merging-layer1
                                         layer-name2 merging-layer2 ...}].

   Writes to these returned layers will automatically update each other as needed to keep the merged
   views consistent."
  [merge-layer layers]
  [(make merge-layer layers ruminate-merge)
   (map-vals layers
             (fn [base]
               (make base [[:phantom (child base :phantom)]
                           [:merge merge-layer]]
                     ruminate-merging)))])


#_(merged m {:tree (parent/make tree-base {:phantom tree-phantom})
             :profile-data (parent/make data-base {:phantom data-phantom})})
