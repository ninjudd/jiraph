(ns jiraph.tc
  (:use jiraph.utils)
  (:import tokyocabinet.HDB))

(set! *warn-on-reflection* true)

(defn- tc-open [path flags]
  (let [db #^HDB (HDB.)]
    (if (.open db path flags)
      db
      (println path "open error:" (HDB/errmsg (.ecode db))))))

(defn db-open [path & args]
  (let [opts   (apply hash-map args)
        flags  (bit-or HDB/OWRITER HDB/OCREAT)]
    (-> opts
        (assoc-or :key-fn #(.getBytes (str %)))
        (assoc-or :val-fn #(.getBytes (str %)))
        (assoc :nodes (tc-open (str path "/nodes") flags))
        (assoc :edges (tc-open (str path "/edges") flags)))))

(defn db-set [env name key val]
  (if (let [key #^bytes ((env :key-fn) key)
            val #^bytes ((env :val-fn) val)
            db  #^HDB (env name)]
        (.put db key val))
    val))

(defn db-add [env name key val]
  (if (let [key #^bytes ((env :key-fn) key)
            val #^bytes ((env :val-fn) val)
            db  #^HDB (env name)]
        (.putkeep db key val))
    val))

(defn db-get [env name key]
  (let [key #^bytes ((env :key-fn) key)
        db  #^HDB (env name)]
    (.get db key)))

(defn db-update [env name key fn]
  (let [db  #^HDB (env name)]
    (.tranbegin db)
    (let [old #^bytes (db-get env name key)
          new (fn old)]
      (if (nil? new)
        (.tranabort db)
        (do (db-set env name key new)
            (.trancommit db)))
      new)))

(defn db-delete [env name key]
  (let [key #^bytes ((env :key-fn) key)
        db  #^HDB (env name)]
    (.out db key)))
