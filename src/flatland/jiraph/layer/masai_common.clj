(ns flatland.jiraph.layer.masai-common
  (:use [flatland.useful.utils :only [or-max]]
        [flatland.useful.state :only [put!]])
  (:require [flatland.masai.db :as db]
            [flatland.retro.core :as retro])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream
            DataOutputStream DataInputStream]))

(defn bytes->long [bytes]
  (-> bytes (ByteArrayInputStream.) (DataInputStream.) (.readLong)))

(defn long->bytes [long]
  (-> (ByteArrayOutputStream. 8)
      (doto (-> (DataOutputStream.) (.writeLong long)))
      (.toByteArray)))

(def revision-key (byte-array [(byte 0)]))

(defn revision-key? [key]
  (and (= 1 (alength key))
       (= 0 (aget key 0))))

(let [freshen (fn [cache layer]
                (or @cache
                    (put! cache
                          (if-let [bytes (db/fetch (:db layer) revision-key)]
                            (bytes->long bytes)
                            0))))]
  (defn implement-ordered [class]
    (extend class
      retro/OrderedRevisions
      {:revision-range (fn [this]
                         (let [cache (:max-written-revision this)]
                           [(or @cache
                                (locking cache
                                  (freshen cache this)))]))
       :touch (fn [this]
                (when-let [revision (:revision this)]
                  (let [cache (:max-written-revision this)]
                    (locking cache
                      (let [old-max (freshen cache this)]
                        (when (> revision old-max)
                          (db/put! (:db this) revision-key
                                   (long->bytes revision))
                          (put! cache revision)))))))})))

(defn revision-to-read [layer]
  (let [revision (:revision layer)]
    (and revision
         (let [[max-written] (retro/revision-range layer)]
           (when (< revision max-written)
             revision)))))
