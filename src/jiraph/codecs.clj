(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]])
  (:require [gloss.io   :as io]
            [gloss.core :as gloss]))

(defn encode [codec val opts] ; (opts -> Codec) -> Deserialized -> opts -> [Byte]
  (io/encode (codec opts) val))

(defn decode [codec data opts]
  (io/decode (codec opts) data))

(defn revisioned-codec [codec reduce-fn] ;; Codec -> (a -> a) -> (opts -> Codec)
  ;; TODO take in a map of reduce-fn and (optionally) init-val
  (letfn [(reducer [acc x]
            (if (:_reset x), x, (reduce-fn acc x)))
          (combine [items]
            (when (seq items)
              (let [node (reduce reducer items)]
                (with-meta (dissoc node :_rev :_reset)
                  {:revision (:_rev node)}))))
          (frame [pre-encode post-decode]
            (gloss/compile-frame codec
                                 (comp list pre-encode)
                                 (comp combine post-decode)))]
    (-> (fn [{:keys [revision]}]
          (if revision
            (frame #(assoc % :_rev revision)
                   (fn [vals]
                     (take-while #(<= (:_rev %) revision)
                                 vals)))
            (frame identity identity)))
        (with-meta {:reduce-fn reduce-fn
                    :revisions (gloss/compile-frame codec
                                                    nil ;; never write with this codec
                                                    (partial map :_rev))}))))
