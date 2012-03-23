(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned-codec tidy-up revisions-only]]
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
           codec-builder (constantly proto-codec)
           revisioned (revisioned-codec codec-builder reduce-fn)]
       (if (= adjoin reduce-fn)
         (let [full-codec (protobuf/protobuf-codec proto)
               tidy-full (-> (gloss/compile-frame full-codec
                                                  identity, tidy-up)
                             (copy-meta full-codec)
                             (vary-meta assoc :reduce-fn adjoin))
               codec-fn (fn [{:keys [revision] :as opts}]
                          (if (nil? revision)
                            tidy-full
                            (revisioned opts)))]
           (-> codec-fn
               (copy-meta proto-codec)
               (vary-meta assoc
                          :revisions (revisions-only codec-fn)
                          :reduce-fn adjoin)))
         (-> revisioned (copy-meta proto-codec))))))
