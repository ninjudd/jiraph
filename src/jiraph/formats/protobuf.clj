(ns jiraph.formats.protobuf
  (:use [jiraph.formats :only [revisioned-format tidy-node add-revisioning-modes]]
        [jiraph.codex :only [encode decode] :as codex]
        [useful.utils :only [adjoin]]
        [useful.map :only [keyed update update-each]]
        [io.core :only [catbytes]]
        [protobuf.core :only [protodef protobuf-dump]])
  (:require [gloss.core :as gloss]
            [protobuf.codec :as protobuf]))

(def ^:private ^:const len-key :proto_length)

(defn- wrap-tidying [f]
  (fn [opts]
    (update-each (f opts) [:codec] codex/wrap identity tidy-node)))

(defn- wrap-revisioning-modes [f]
  (fn [opts]
    (add-revisioning-modes (f opts))))

(defn- num-bytes-to-encode-length [proto]
  (let [proto (protodef proto)
        min   (alength (protobuf-dump proto {len-key 0}))
        max   (alength (protobuf-dump proto {len-key Integer/MAX_VALUE}))]
    (letfn [(check [test msg]
              (when-not test
                (throw (Exception. (format "In %s: %s %s"
                                           (.getFullName proto) (name len-key) msg)))))]
      (check (pos? min)
             "field is required for repeated protobufs")
      (check (= min max)
             "must be of type fixed32 or fixed64")
      max)))

;; NB doesn't currently work if you do a full/optimized read with _reset keys.
;; plan is to fall back to non-optimized reads in that case, but support an
;; option to protobuf-codec to never try an optimized read if you expect _resets

(defn- length-for-revision [node goal-revision header-len]
  (loop [target-len 0,
         [[rev len] :as pairs] (map vector
                                    (:revisions node)
                                    (len-key node))]
    (if (or (not pairs)
            (> rev goal-revision))
      target-len
      (recur (+ len target-len header-len)
             (next pairs)))))

;; TODO temporarily threw away code for handling non-adjoin reduce-fn
(defn protobuf-format
  ([proto]
     (protobuf-format proto adjoin))
  ([proto reduce-fn]
     (when-not (= reduce-fn adjoin)
       (throw (IllegalArgumentException. (format "Unsupported reduce-fn %s" reduce-fn))))
     (let [schema       (protobuf/codec-schema proto)
           codec        (protobuf/protobuf-codec proto)
           proto-format (keyed [codec schema reduce-fn])
           header-len   (num-bytes-to-encode-length proto)]
       (wrap-revisioning-modes
        (wrap-tidying
         (fn [{:keys [revision] :as opts}]
           (if (nil? revision)
             proto-format
             (update proto-format :codec
                     (fn [codec]
                       {:read (fn [bytes]
                                (let [full-node (decode codec bytes)
                                      goal-length (length-for-revision full-node
                                                                       revision header-len)]
                                  (if (= goal-length (count bytes))
                                    full-node
                                    (let [read-target (byte-array goal-length)]
                                      (System/arraycopy bytes 0 read-target 0 goal-length)
                                      (decode codec read-target)))))
                        :write (fn [node]
                                 (let [node (assoc node :revisions revision)
                                       encoded (encode codec node)]
                                   (catbytes (encode codec {len-key (count encoded)})
                                             encoded)))})))))))))
