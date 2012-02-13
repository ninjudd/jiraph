(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]])
  (:require [gloss.io   :as io]
            [gloss.core :as gloss]
            [ego.core   :as ego]))

(defn encode [codec val opts] ; (opts -> Codec) -> Deserialized -> opts -> [Byte]
  (io/encode (codec opts) val))

(defn decode [codec data opts]
  (io/decode (codec opts) data))

(defn revisioned-codec [codec-builder reduce-fn] ;; Codec -> (a -> a) -> (opts -> Codec) ;; TODO fix
  ;; TODO take in a map of reduce-fn and (optionally) init-val
  (letfn [(reducer [acc x]
            (if (:_reset x), x, (reduce-fn acc x)))
          (combine [items]
            (when (seq items)
              (let [node (reduce reducer items)]
                (with-meta (dissoc node :_rev :_reset)
                  {:revision (:_rev node)}))))]
    (-> (fn [{:keys [revision reset] :as opts}]
          (let [codec (codec-builder opts)
                maybe-reset (if reset
                              (fn [data] (assoc data :_reset true))
                              identity)
                frame (fn [pre-encode post-decode]
                        (-> (gloss/compile-frame codec
                                                 (comp list pre-encode maybe-reset)
                                                 (comp combine post-decode))
                            (vary-meta assoc :reduce-fn reduce-fn)))]
            (if revision
              (frame #(assoc % :_rev revision)
                     (fn [vals]
                       (take-while #(<= (:_rev %) revision)
                                   vals)))
              (frame identity identity))))
        (with-meta {:reduce-fn reduce-fn
                    :revisions (fn [opts]
                                 (gloss/compile-frame (codec-builder opts)
                                                      nil ;; never write with this codec
                                                      (partial map :_rev)))}))))

(defn wrap-typing [codec-fn types]
  (fn [{:keys [id] :as opts}]
    (let [codec (codec-fn opts)]
      (if (types (ego/type-key id))
        codec
        (vary-meta codec dissoc :schema)))))
