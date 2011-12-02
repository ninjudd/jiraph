(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]])
  (:require [gloss.io   :as io]
            [gloss.core :as gloss]))

(defn encode [codec val opts] ; (opts -> Codec) -> Deserialized -> opts -> [Byte]
  (io/encode (codec opts) val))

(defn decode [codec data opts]
  (io/decode (codec opts) data))

(defn revisioned-codec [codec reduce-fn] ;; Codec -> (a -> a) -> (opts -> Codec)
  (let [combine (partial reduce reduce-fn)
        frame (fn [pre-encode post-decode]
                (gloss/compile-frame codec
                                     (comp list pre-encode)
                                     (comp combine post-decode)))]
    (-> (fn [{:keys [revision]}]
          (if revision
            (frame #(assoc % :revisions [revision])
                   (fn [vals]
                     (take-while #(<= (peek (:revisions %)) revision)
                                 vals)))
            (frame identity identity)))
        (with-meta {:reduce-fn reduce-fn}))))
