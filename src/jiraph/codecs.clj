(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]]
        [useful.experimental :only [lift-meta]]
        [useful.map :only [update]]
        [useful.utils :only [copy-meta]])
  (:require [gloss.io :as io]
            [gloss.core :as gloss]
            [ego.core :as ego]
            [schematic.core :as schema]))

(def reset-key :codec_reset)
(def revision-key :revisions)
(def len-key :proto_length)

(defn tidy-node [node]
  (-> node
      (dissoc reset-key len-key)
      (lift-meta revision-key)))

(defn tidy-schema [codec]
  (vary-meta codec update :schema
             schema/dissoc-fields revision-key))

(defn encode [codec val opts] ; (opts -> Codec) -> Deserialized -> opts -> [Byte]
  (io/encode (codec opts) val))

(defn decode [codec data opts]
  (io/decode (codec opts) data))

(defn revisions-only [codec-fn]
  (fn [opts] ;; read revisions just by discarding the rest
    (gloss/compile-frame (codec-fn opts)
                         nil ;; never write with this codec
                         (comp revision-key meta))))

(defn revisioned-codec [codec-builder reduce-fn] ;; Codec -> (a -> a) -> (opts -> Codec) ;; TODO fix
  ;; TODO take in a map of reduce-fn and (optionally) init-val
  (letfn [(reducer [acc x]
            (reduce-fn (if (reset-key x)
                         (select-keys acc [revision-key])
                         acc)
                       (dissoc x reset-key)))
          (combine [items]
            (when (seq items)
              (tidy-node (or (reduce reducer items) {}))))]
    (letfn [(node-codec [{reset reset-key, :keys [revision] :as opts}]
              (let [codec (codec-builder opts)
                    maybe-reset (if reset
                                  (fn [data] (assoc data reset-key true))
                                  identity)
                    frame (fn [pre-encode post-decode]
                            (-> (gloss/compile-frame codec
                                                     (comp list pre-encode maybe-reset)
                                                     (comp combine post-decode))
                                (copy-meta codec)
                                (tidy-schema)))]
                (if revision
                  (frame #(assoc % revision-key [revision])
                         (fn [vals]
                           ;; run BEFORE tidy-node, so revision is not in the meta but in the node
                           (take-while #(<= (first (revision-key %)) revision)
                                       vals)))
                  (frame identity identity))))]
      (-> node-codec
          (with-meta {:reduce-fn reduce-fn
                      :revisions (revisions-only node-codec)})))))

(def ^{:dynamic true, :doc "When bound to false, codecs created by wrap-typing will ignore types."}
  *honor-layer-types* true)

(defn wrap-typing [codec-fn accept-id?]
  (-> (fn [{:keys [id] :as opts}]
        (when (or (not *honor-layer-types*)
                  (accept-id? id))
          (codec-fn opts)))
      (copy-meta codec-fn)))
