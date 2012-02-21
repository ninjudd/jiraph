(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned-codec]]
        [useful.utils :only [adjoin copy-meta]])
  (:require [protobuf.codec :as protobuf]))

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
       (-> (if (= adjoin reduce-fn)
             (let [full (protobuf/protobuf-codec proto)]
               (fn [{:keys [revision] :as opts}]
                 (if (nil? revision)
                   full
                   (revisioned opts))))
             revisioned)
           (copy-meta proto-codec)))))
