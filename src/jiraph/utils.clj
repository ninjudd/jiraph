(ns jiraph.utils
  (:use [clojure.string :only [join]]
        [clojure.core.match :only [match]]))

(defn meta-id?
  "Is the given id referring to a meta node or layer meta?"
  [id]
  (vector? id))

(defn meta-id
  "Make an id that refers to a meta node or layer meta."
  [id]
  (when id
    [id]))

(defn base-id
  "Return the underlying id for a meta-id."
  [id]
  (if (meta-id? id)
    (first id)
    id))

(defn meta-keyseq?
  "Does the given keyseq start with a meta id?"
  [keyseq]
  (meta-id? (first keyseq)))

(defn id->str
  "Convert an id to a database key string."
  [id]
  (if (meta-id? id)
    (str "_" (base-id id))
    (str id)))

(defn keyseq->str
  "Convert a key sequence to a database key string."
  [[id & keys]]
  (join ":"
        (cons (id->str id)
              (map name keys))))

(defn edges-keyseq [keyseq]
  (if (meta-keyseq? keyseq)
    (match keyseq
      [_]           [:incoming]
      [_ :incoming] [])
    (match keyseq
      [_]        [:edges]
      [_ :edges] [])))

(defn deleted-edge-keyseq [keyseq]
  (if (meta-keyseq? keyseq)
    (match keyseq
      [_ :incoming] [])
    (match keyseq
      [_ :edges _]          [:deleted]
      [_ :edges _ :deleted] [])))

(defn deleted-node-keyseq [keyseq]
  (when-not (meta-keyseq? keyseq)
    (match keyseq
      [_]          [:deleted]
      [_ :deleted] [])))