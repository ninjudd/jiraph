(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]])
  (:require [gloss.io :as gloss]))

(defn encode [codec val opts]
  (gloss/encode (codec opts) val))

(defn decode [codec data opts]
  (gloss/decode (codec opts) data))

(defn revisioned-codec [codec reduce-fn]
  (-> (fn [{:keys [revision]}]
        (gloss/compile-frame codec
                             #(assoc % :revisions [revision])
                             (fn [vals]
                               (->> vals
                                    (take-while #(<= (peek (:revisions %)) revision))
                                    (reduce reduce-fn)))))
      (with-meta {:reduce-fn reduce-fn})))
