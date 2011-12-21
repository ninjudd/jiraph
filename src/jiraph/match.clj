(ns jiraph.match
  (:require [clojure.string :as s]
            [io.core :as io])
  (:use useful.debug
        [useful.utils :only [adjoin pop-if]]
        [masai.db :only [fetch-seq]])
  (:import (java.nio ByteBuffer)))

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
    (for [[k v] (if (= :* k)
                  node ;; each k/v in the node
                  (select-keys node [k])) ;; just the one
          path (matching-subpaths v ks)]
      (cons k path))
    '(())))

;; TODO this is hella inefficient in the way it does dissoc-in/get-in
(defn write-node [node writers]
  (second (reduce (fn [[node outputs] [path writer]]
                    (if (seq path)
                      (reduce (fn [[node outputs] p]
                                [(no-nil-update node (pop p) #(dissoc % (peek p)))
                                 (conj outputs (writer p (get-in node p)))])
                              [node outputs]
                              (map vec (matching-subpaths node path)))
                      [{} (conj outputs (writer [] node))]))
                  [node []]
                  writers)))

(defn parse-bytes [b]
  (read-string (String. (io/bufseq->bytes b))))
