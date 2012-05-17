(ns ruminate.core)

(defn add-ruminant [source name init reduce-fn]
  (vary-meta source assoc-in
             [::ruminants name] {:agent     (agent init)
                                 :reduce-fn reduce-fn}))

(defn ruminant [source name]
  (let [agent (get-in (meta source) [::ruminants name :agent])]
    (await agent)
    @agent))

(defn ruminate [source value]
  (doseq [[name ruminant] (::ruminants (meta source))]
    (send (:agent ruminant) (:reduce-fn ruminant) value)))
