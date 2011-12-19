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

(defn substring-after [^String delim]
  (fn [^String s]
    (subs s (inc (.lastIndexOf s delim)))))

(let [char-after (fn [c]
                   (char (inc (int c))))
      after-colon (char-after \:)
      str-after (fn [s] ;; the string immediately after this one in lexical order
                  (str s \u0001))]
  (defn bounds [path]
    (let [path       (vec path)
          last       (peek path)
          multi?     (= :* last)
          path       (pop path)
          top-level? (empty? path)
          start      (s/join ":" (map name path))]
      (if top-level?
        {:start last, :stop (str-after last)
         :keyfn (constantly last), :parent []}
        (into {:parent path}
              (if multi?
                {:start (str start ":")
                 :stop (str start after-colon)
                 :keyfn (substring-after ":")}
                (let [start-key (str start ":" (name last))]
                  {:start start-key
                   :stop (str-after start-key)
                   :keyfn (constantly last)})))))))

(defn assoc-levels
  "Like assoc-in, but an empty keyseq replaces whole map."
  [m ks v]
  (if-let [[k & ks] (seq ks)]
    (assoc m k (assoc-levels (get m k) ks v))
    v))

(defn read-node [db id codecs]
  (reduce adjoin
          (for [[path codec] codecs
                :let [{:keys [start stop parent keyfn]} (bounds (cons id path))
                      kvs (seq (for [[k v] (fetch-seq db start)
                                     :while (neg? (compare k stop))]
                                 [(keyfn k) (codec [(ByteBuffer/wrap v)])]))]
                :when kvs]
            (assoc-levels {} parent
                          (into {} kvs)))))

(defn parse-bytes [b]
  (read-string (String. (io/bufseq->bytes b))))
