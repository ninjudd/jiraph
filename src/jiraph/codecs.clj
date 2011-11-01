(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]])
  (:require [gloss.io :as gloss]))

(defn encode [format val & args]
  (gloss/encode (if (fn? format)
                  (apply format args)
                  format)
                val))

(defn decode [format data & args]
  (gloss/decode (if (fn? format)
                  (apply format args)
                  format)
                data))

(deftype RevisionedCodec [codec reduce-fn revision]
  Reader
  (read-bytes [this buf-seq]
    (let [[success vals remainder] (read-bytes codec buf-seq)]
      (if success
        [true
         (reduce reduce-fn
                 (if revision
                   (take-while #(<= (last (:revisions %)) revision) vals)
                   vals))
         remainder]
        [false vals remainder])))

  Writer
  (sizeof [this]
    (sizeof codec))
  (write-bytes [this buf val]
    (write-bytes codec buf (list (assoc val :revisions [revision])))))

(defn revisioned [codec reduce-fn]
  (fn [revision]
    (RevisionedCodec. codec reduce-fn revision)))
