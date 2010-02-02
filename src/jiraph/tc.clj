(ns jiraph.tc
  (:use jiraph.utils)
  (:import tokyocabinet.HDB))

(set! *warn-on-reflection* true)

(defn- open [path opts]
  (let [db     #^HDB (HDB.)
        bnum   (or (opts :bnum) 0)
        apow   (or (opts :apow) -1)
        fpow   (or (opts :fpow) -1)
        tflags (bit-or
                (if (opts :large) HDB/TLARGE 0)
                (condp = (opts :compress)
                  :deflate HDB/TDEFLATE
                  :bzip    HDB/TBZIP
                  :tcbs    HDB/TTCBS
                  nil      0))
        oflags (reduce bit-or
                 (list (if (opts :create)   HDB/OCREAT  0)
                       (if (opts :truncate) HDB/OTRUNC  0)
                       (if (opts :readonly) HDB/OREADER HDB/OWRITER)
                       (if (opts :tsync)    HDB/OTSYNC  0)
                       (if (opts :nolock)   HDB/ONOLCK  0)
                       (if (opts :noblock)  HDB/OLCKNB  0)))]
    (when-not (.tune db bnum apow fpow tflags)
      (println path "tune error:" (HDB/errmsg (.ecode db))))
    (when-not (.open db path oflags)
      (println path "open error:" (HDB/errmsg (.ecode db))))
    db))

(defn db-open [path & args]
  (let [opts (args-map args)]
    (-> opts
        (assoc-or :key-fn #(.getBytes (str %)))
        (assoc-or :val-fn #(.getBytes (str %)))
        (assoc :nodes (open (str path "/nodes") (merge opts (opts :node-opts))))
        (assoc :edges (open (str path "/edges") (merge opts (opts :edge-opts)))))))

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
