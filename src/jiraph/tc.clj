(ns jiraph.tc
  (:use jiraph.utils)
  (:import tokyocabinet.HDB))

(set! *warn-on-reflection* true)

(defclass DB :tables :opts :key-fn :val-fn :node-proto :edge-list-proto)

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
        oflags (reduce bit-or HDB/OCREAT
                 (list (if (opts :truncate) HDB/OTRUNC  0)
                       (if (opts :readonly) HDB/OREADER HDB/OWRITER)
                       (if (opts :tsync)    HDB/OTSYNC  0)
                       (if (opts :nolock)   HDB/ONOLCK  0)
                       (if (opts :noblock)  HDB/OLCKNB  0)))]
    (when-not (.tune db bnum apow fpow tflags)
      (println path "tune error:" (HDB/errmsg (.ecode db))))
    (when-not (.open db path oflags)
      (println path "open error:" (HDB/errmsg (.ecode db))))
    db))

(defn- db-table [env table]
  (or (table @(env :tables))
      (dosync
       (let [opts (env :opts)
             db   (open (str (opts :path) "/" (name table)) opts)]
         (alter (env :tables) assoc table db)
         db))))

(defn db-open [args]
  (let [opts (args-map args)]
    (DB :tables (ref {})
        :opts   opts
        :key-fn (or (opts :key-fn) #(.getBytes (str %)))
        :val-fn (or (opts :val-fn) #(.getBytes (str %)))
        :node-proto (opts :node-proto)
        :edge-list-proto (opts :edge-list-proto))))

(defn db-set [env table key val]
  (if (let [key #^bytes ((env :key-fn) key)
            val #^bytes ((env :val-fn) val)
            db  #^HDB (db-table env table)]
        (.put db key val))
    val))

(defn db-add [env table key val]
  (if (let [key #^bytes ((env :key-fn) key)
            val #^bytes ((env :val-fn) val)
            db  #^HDB (db-table env table)]
        (.putkeep db key val))
    val))

(defn db-get [env table key]
  (let [key #^bytes ((env :key-fn) key)
        db  #^HDB (db-table env table)]
    (.get db key)))

(defn db-update [env table key fn]
  (let [db #^HDB (db-table env table)]
    (.tranbegin db)
    (let [old #^bytes (db-get env table key)
          new (fn old)]
      (if (nil? new)
        (.tranabort db)
        (do (db-set env table key new)
            (.trancommit db)))
      new)))

(defn db-delete [env table key]
  (let [key #^bytes ((env :key-fn) key)
        db  #^HDB (db-table env table)]
    (.out db key)))
