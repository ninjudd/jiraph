(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned-codec tidy-node codec-meta]]
        [useful.utils :only [adjoin copy-meta]]
        [useful.experimental :only [lift-meta]])
  (:require [protobuf.codec :as protobuf]
            [gloss.core :as gloss]))

;; NB doesn't currently work if you do a full/optimized read with _reset keys.
;; plan is to fall back to non-optimized reads in that case, but support an
;; option to protobuf-codec to never try an optimized read if you expect _resets
(defn protobuf-codec
  ([proto]
     (protobuf-codec proto adjoin))
  ([proto reduce-fn]
     (let [proto-codec (protobuf/protobuf-codec proto :repeated true)
           revisioned  (revisioned-codec (constantly proto-codec) reduce-fn)]
       (if (= adjoin reduce-fn)
         (let [codec (protobuf/protobuf-codec proto)
               full  (-> (gloss/compile-frame codec identity tidy-node)
                         (codec-meta codec adjoin))]
           (fn [{:keys [revision] :as opts}]
             (if (nil? revision)
               full
               (revisioned opts))))
         revisioned))))
