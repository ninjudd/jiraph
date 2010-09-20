(ns jiraph.byte-append-layer
  (:refer-clojure :exclude [sync count])
  (:use jiraph.layer
        [useful :only [update verify zip filter-vals remove-vals]])
  (:require [jiraph.byte-database :as db]
            [jiraph.byte-append-format :as f]
            [jiraph.reader-append-format :as reader-append-format]
            [jiraph.protobuf-append-format :as protobuf-append-format])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(def *transactions* #{})

(def meta-prefix "_")
(defn- meta-key [id]
  (str meta-prefix id))

(declare meta-len)
(defn- get-meta* [layer key rev]
  (let [db (.db layer)
        mf (.meta-format layer)]
    (if rev
      (f/load mf (db/get db key) (meta-len key rev))
      (f/load mf (db/get db key)))))

(defn find-with
  "Returns the val corresponding to the first key where (pred key) returns true."
  [pred keys vals]
  (last (first (filter (comp pred first) (zip keys vals)))))

(defn- len [layer id rev]
  (if-not rev
    (db/len (.db layer) id)
    (let [meta (get-meta* layer (meta-key id) nil)]
      (find-with (partial >= rev)
                 (reverse (:rev meta))
                 (reverse (:len meta))))))

(defn- meta-len [layer key rev]
  (if-not rev
    (db/len (.db layer) key)
    (let [meta (get-meta* layer key nil)]
      ;; Must shift meta lengths by one since they store the length of the previous revision.
      (find-with (partial >= rev)
                 (reverse (:mrev meta))
                 (cons nil (reverse (:mlen meta)))))))

(defn- append-meta! [layer id attrs & [rev]]
  (let [db (.db layer)
        mf (.meta-format layer)
        key (meta-key id)]
    (transaction layer
      (->> (if rev
             (let [len (db/len db key)]
               (assoc attrs :mrev rev :mlen len))
             attrs)
           (f/dump mf)
           (db/append! db key)))))

(defn- set-incoming! [layer exists from-id to-ids]
  (let [attrs {:in {from-id exists}}]
    (doseq [to-id to-ids]
      (append-meta! layer to-id attrs *rev*))))

(defn- set-len! [layer id len]
  (if *rev*
    (append-meta! layer id {:rev *rev* :len len})))

(defn- reset-len! [layer id & [len]]
  ;; Insert a nil length to indicate that all lengths before the current one are no longer valid.
  (append-meta! layer id
    (if (and *rev* len)
      {:rev [0 *rev*] :len [nil len]}
      {:rev 0 :len nil})))

(def count-key (str meta-prefix meta-prefix "count"))

(defn- inc-count! [layer]
  (db/inc! (.db layer) count-key 1))

(defn- dec-count! [layer]
  (db/inc! (.db layer) count-key -1))

(defn- make-node [attrs]
  (if *rev*
    (assoc attrs :rev *rev*)
    (dissoc attrs :rev)))

(deftype ByteAppendLayer [db format meta-format]
  jiraph.layer/Layer

  (open  [layer] (db/open  db))
  (close [layer] (db/close db))
  (sync! [layer] (db/sync! db))

  (node-count [layer]
    (db/inc! db count-key 0))

  (node-ids [layer]
    (remove #(.startsWith % meta-prefix) (db/key-seq db)))

  (get-node [layer id]
    (if-let [length (if *rev* (len layer id *rev*))]
      (f/load format (db/get db id) length)
      (f/load format (db/get db id))))

  (get-meta [layer id]
    (get-meta* layer (meta-key id) *rev*))

  (node-exists? [layer id]
    (< 0 (len layer id *rev*)))

  (txn [layer f]
    (if (contains? *transactions* layer)
      (f)
      (binding [*transactions* (conj *transactions* layer)]
        (db/txn-begin db)
        (try (let [result (f)]
               (db/txn-commit db)
               result)
             (catch Exception e
               (db/txn-abort db)
               (throw e))))))

  (add-node! [layer id attrs]
    (let [node (make-node attrs)
          data (f/dump format node)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (set-incoming! layer true id (keys (:edges attrs)))
        attrs)))

  (append-node! [layer id attrs]
    (when-not (empty? attrs)
      (transaction layer
        (let [len  (db/len db id)
              node (make-node attrs)
              data (f/dump format node)]
          (db/append! db id data)
          (if (= -1 len)
            (inc-count! layer))
          (set-len! layer id (+ (max len 0) (alength data)))
          (set-incoming! layer true  id (keys (remove-vals :deleted (:edges attrs))))
          (set-incoming! layer false id (keys (filter-vals :deleted (:edges attrs))))
          node))))

  (update-node! [layer id f args]
    (transaction layer
      (let [old  (get-node layer id)
            new  (make-node (apply f old args))
            data (f/dump format new)]
        (db/put! db id data)
        (if (nil? old)
          (inc-count! layer))
        (reset-len! layer id (alength data))
        (let [new-edges (set (keys (remove-vals :deleted (:edges new))))
              old-edges (set (keys (remove-vals :deleted (:edges old))))]
          (set-incoming! layer true  id (remove old-edges new-edges))
          (set-incoming! layer false id (remove new-edges old-edges)))
        new)))

  (assoc-node! [layer id attrs]
    (update-node! layer id merge [attrs]))

  (compact-node! [layer id]
    (update-node! layer id update [:edges (partial remove-vals :deleted)]))

  (delete-node! [layer id]
    (transaction layer
      (let [node (get-node layer id)]
        (when (db/delete! db id)
          (dec-count! layer)
          (reset-len! layer id)
          (set-incoming! layer false id (keys (:edges node)))))))

  (truncate! [layer] (db/truncate! db)))

(defn make [db format & [meta-format]]
  (ByteAppendLayer.
   db format
   (or meta-format
       (cond (instance? jiraph.reader-append-format.ReaderAppendFormat format)
             (reader-append-format/make {:in #{} :rev [] :len [] :mrev [] :mlen []})
             (instance? jiraph.protobuf-append-format.ProtobufAppendFormat format)
             (protobuf-append-format/make jiraph.Meta$Node)))))
