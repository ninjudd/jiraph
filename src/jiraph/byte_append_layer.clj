(ns jiraph.byte-append-layer
  (:refer-clojure :exclude [sync count])
  (:use jiraph.layer
        [useful :only [find-with]])
  (:require [jiraph.byte-database :as db]
            [jiraph.byte-append-format :as f]
            [jiraph.reader-append-format :as reader-append-format]
            [jiraph.protobuf-append-format :as protobuf-append-format])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(def meta-prefix     "_")
(def internal-prefix "__")
(def property-prefix "___")

(defn- meta-key [id]
  (str meta-prefix id))

(def count-key (str internal-prefix "count"))

(defn- property-key [key]
  (str property-prefix key))

(declare meta-len)
(defn- get-meta [layer key rev]
  (let [db (.db layer)
        mf (.meta-format layer)]
    (if rev
      (f/load mf (db/get db key) 0 (meta-len layer key rev))
      (f/load mf (db/get db key)))))

(defn- len
  "The byte length of the node at revision rev."
  [layer id rev]
  (if-not rev
    (db/len (.db layer) id)
    (let [meta (get-meta layer (meta-key id) nil)]
      (find-with (partial >= rev)
                 (reverse (:rev meta))
                 (reverse (:len meta))))))

(defn- meta-len
  "The byte length of the meta node at revision rev."
  [layer key rev]
  (or (if rev
        (let [meta (get-meta layer key nil)]
          ;; Must shift meta lengths by one since they store the length of the previous revision.
          (find-with (partial >= rev)
                     (reverse (:mrev meta))
                     (cons nil (reverse (:mlen meta))))))
      (db/len (.db layer) key)))

(defn- append-meta! [layer id attrs & [rev]]
  (let [db (.db layer)
        mf (.meta-format layer)
        key (meta-key id)]
    (->> (if rev
           (assoc attrs :mrev rev :mlen (db/len db key))
           attrs)
         (f/dump mf)
         (db/append! db key))))

(defn- set-len! [layer id len]
  (if *rev*
    (append-meta! layer id {:rev *rev* :len len})))

(defn- reset-len! [layer id & [len]]
  ;; Insert a nil length to indicate that all lengths before the current one are no longer valid.
  (append-meta! layer id
    (if (and *rev* len)
      {:rev [0 *rev*] :len [nil len]}
      {:rev 0 :len nil})))

(defn- inc-count! [layer]
  (db/inc! (.db layer) count-key 1))

(defn- dec-count! [layer]
  (db/inc! (.db layer) count-key -1))

(defn- make-node [attrs]
  (let [attrs (dissoc attrs :id)]
    (if *rev*
      (assoc attrs :rev *rev*)
      (dissoc attrs :rev))))

(deftype ByteAppendLayer [db format meta-format]
  jiraph.layer/Layer

  (open  [layer] (db/open  db))
  (close [layer] (db/close db))
  (sync! [layer] (db/sync! db))

  (node-count [layer]
    (db/inc! db count-key 0))

  (node-ids [layer]
    (remove #(.startsWith % meta-prefix) (db/key-seq db)))

  (fields [layer]
    (remove #(or (contains? #{:id :edges :rev} %)
                 (.startsWith (str %) "_"))
            (f/fields format)))

  (get-property  [layer key]
    (if-let [bytes (db/get db (property-key key))]
      (read-string (String. bytes))))

  (set-property! [layer key val]
    (let [bytes (.getBytes (pr-str val))]
      (db/put! db (property-key key) bytes)
      val))

  (get-node [layer id]
    (if-let [node (if-let [length (if *rev* (len layer id *rev*))]
                    (f/load format (db/get db id) 0 length)
                    (f/load format (db/get db id)))]
      node))

  (node-exists? [layer id]
    (< 0 (len layer id *rev*)))

  (wrap-transaction [layer f]
    (fn []
      (db/txn-begin db)
      (try (let [result (f)]
             (db/txn-commit db)
             result)
           (catch Throwable e
             (db/txn-abort db)
             (throw e)))))

  (add-node! [layer id attrs]
    (let [node (make-node attrs)
          data (f/dump format node)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (f/load format data))))

  (append-node! [layer id attrs]
    (when-not (empty? attrs)
      (let [len  (db/len db id)
            node (make-node attrs)
            data (f/dump format node)]
        (db/append! db id data)
        (if (= -1 len)
          (inc-count! layer))
        (set-len! layer id (+ (max len 0) (alength data)))
        (f/load format data))))

  (update-node! [layer id f args]
    (let [old  (get-node layer id)
          new  (make-node (apply f old args))
          data (f/dump format new)]
      (db/put! db id data)
      (if (nil? old)
        (inc-count! layer))
      (reset-len! layer id (alength data))
      [old (f/load format data)]))

  (delete-node! [layer id]
    (let [node (get-node layer id)]
      (when (db/delete! db id)
        (dec-count! layer)
        (reset-len! layer id)
        node)))

  (get-revisions [layer id] (:rev (get-meta layer (meta-key id) *rev*)))
  (get-incoming  [layer id] (:in  (get-meta layer (meta-key id) *rev*)))

  (add-incoming!  [layer id from-id] (append-meta! layer id {:in {from-id true}}  *rev*))
  (drop-incoming! [layer id from-id] (append-meta! layer id {:in {from-id false}} *rev*))

  (truncate! [layer] (db/truncate! db)))

(defn make [db & [format meta-format]]
  (let [format (or format (reader-append-format/make))]
    (ByteAppendLayer.
     db format
     (or meta-format
         (cond (instance? jiraph.reader-append-format.ReaderAppendFormat format)
               (reader-append-format/make {:in #{} :rev [] :len [] :mrev [] :mlen []})
               (instance? jiraph.protobuf-append-format.ProtobufAppendFormat format)
               (protobuf-append-format/make jiraph.Meta$Node))))))
