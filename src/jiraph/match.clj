(ns jiraph.match
  (:use useful.debug))

(defn no-nil-update [m ks f]
  (if-let [[k & ks] (seq ks)]
    (let [v (no-nil-update (get m k) ks f)]
      (if (and (not (nil? v))
               (or (not (coll? v))
                   (seq v)))
        (assoc m k v)
        (dissoc m k)))
    (f m)))

;; TODO still needs work
(defn matching-subpaths [node path]
  (if-let [[k & ks] (seq path)]
    (if (= :* k)
      (for [[k v] node
            path (matching-subpaths v ks)]
        (cons k path))
      (when-let [v (get node k)]
        (for [path (matching-subpaths v ks)]
          (cons k path))))
    '(())))

;; TODO doesn't make sense at all
(defn write-node [node writers]
  (reduce (fn [node [path writer]]
            (if (seq path)
              (reduce (fn [node p]
                        (let [data (get-in node p)]
                          (writer p data)
                          (no-nil-update node (pop p) #(dissoc % (peek p)))))
                      node
                      (map vec (matching-subpaths node path)))
              (do (writer [] node)
                  {})))
          node
          writers))
