(ns jiraph.byte-append-layer
  (:refer-clojure :exclude [sync count])
  (:use jiraph.layer
        [useful :only [update verify zip]])
  (:require [jiraph.byte-database :as db]
            [jiraph.byte-append-format :as f])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(defn- meta-key [id]
  (str "_" id))

(declare meta-len)
(defn- get-meta* [layer key rev]
  (let [db (.db layer)
        mf (.meta-format layer)]
    (if rev
      (f/load mf (db/get db key) (meta-len key rev))
      (f/load mf (db/get db key)))))

(defn- len [layer id rev]
  (if-not rev
    (db/len (.db layer) id)
    (let [meta (get-meta* layer (meta-key id) nil)]
      (last (some #(> rev (first %))
                  (zip (reverse (:rev meta))
                       (reverse (:len meta))))))))

(defn- meta-len [layer key rev]
  (if-not rev
    (db/len (.db layer) key)
    (let [meta (get-meta* layer key nil)]
      ;; Must shift meta lengths by one since they store the length of the previous revision.
      (last (some #(> rev (first %))
                  (zip (reverse (:_rev meta))
                       (cons nil (reverse (:_len meta)))))))))

(defn- append-meta! [layer id attrs & [rev]]
  (let [db (.db layer)
        mf (.meta-format layer)
        key (meta-key id)]
    (db/txn db
      #(->> (if rev
              (let [len (db/len db key)]
                (assoc attrs :_rev rev :_len len))
              attrs)
            (f/dump mf)
            (db/append! db key)))))

(defn- set-incoming! [layer exists from-id to-ids]
  (let [attrs {:incoming {from-id exists}}]
    (doseq [to-id to-ids]
      (append-meta! layer to-id attrs *rev*))))

(defn- set-len! [layer id len]
  (if *rev*
    (append-meta! layer id {:rev *rev* :len len})))

(defn- reset-len! [layer id & [len]]
  ;; Insert a nil length to indicate that all lengths before the current one are no longer valid.
  (if *rev*
    (append-meta! layer id
      (if len
        {:rev [0 *rev*] :len [nil len]}
        {:rev 0 :len nil}))))

(defn- inc-count! [layer]
  (db/inc! (.db layer) "__count" 1))

(defn- dec-count! [layer]
  (db/inc! (.db layer) "__count" -1))

(defn filter-vals
  "Returns a map that only contains values where (pred value) returns true."
  [pred map]
  (if map
    (select-keys map (for [[key val] map :when (pred val)] key))))

(defn remove-vals
  "Returns a map that only contains values where (pred value) returns false."
  [pred map]
  (filter-vals (comp not pred) map))

(deftype ByteAppendLayer [db format meta-format]
  jiraph.layer/Layer

  (open      [layer] (db/open  db))
  (close     [layer] (db/close db))
  (sync      [layer] (db/sync  db))
  (truncate! [layer] (db/truncate! db))
  (count     [layer] (/ (db/count db) 2))

  (get-node [layer id]
    (if *rev*
      (f/load format (db/get db id) (len layer id *rev*))
      (f/load format (db/get db id))))

  (get-meta [layer id]
    (get-meta* layer (meta-key id) *rev*))

  (node-exists? [layer id]
    (< 0 (len layer id *rev*)))

  (txn [layer f] (db/txn db f))

  (add-node! [layer id attrs]
    (let [data (f/dump format attrs)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (set-incoming! layer true id (keys (:edges attrs)))
        attrs)))

  (update-node! [layer id f args]
    (txn layer
      #(let [old (get-node layer id)
             new (apply f old args)]
         (let [data (f/dump format new)]
           (db/put! db id data)
           (if (nil? old)
             (inc-count! layer))
           (reset-len! layer id (alength data))
           (let [new-edges (remove-vals :deleted (:edges new))
                 old-edges (remove-vals :deleted (:edges old))]
             (set-incoming! layer true  id (remove old-edges (keys new-edges)))
             (set-incoming! layer false id (remove new-edges (keys old-edges))))
           new))))

  (assoc-node! [layer id args]
    (update-node! layer id assoc args))

  (append-node! [layer id attrs]
    (when-not (empty? attrs)
      (txn layer
        #(let [len  (db/len db id)
               data (f/dump format attrs)]
          (db/append! db id data)
          (if (= -1 len)
            (inc-count! layer))
          (set-len! layer id (+ (max len 0) (alength data)))
          (set-incoming! layer true  id (remove-vals :deleted (:edges attrs)))
          (set-incoming! layer false id (filter-vals :deleted (:edges attrs)))
          attrs))))

  (compact-node! [layer id]
    (update-node! layer id update [:edges (partial remove-vals :deleted)]))

  (delete-node! [layer id]
    (txn layer
      #(let [node (get-node layer id)]
         (when (db/delete! db id)
           (dec-count! layer)
           (reset-len! layer id)
           (set-incoming! layer false id (keys (:edges node)))))))

  (truncate! [layer] (db/truncate! db)))

(defn make [db format meta-format]
  (ByteAppendLayer. db format meta-format))
