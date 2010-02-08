(ns jiraph.tc
  (:use jiraph.utils)
  (:import tokyocabinet.HDB))

(set! *warn-on-reflection* true)

(def default-opts
     {:key  #(.getBytes (str %))
      :dump #(.getBytes (pr-str %))
      :load #(read-string (str %))})

(defn- tflags [opts]
  (bit-or
   (if (opts :large) HDB/TLARGE 0)
   (condp = (opts :compress)
     :deflate HDB/TDEFLATE
     :bzip    HDB/TBZIP
     :tcbs    HDB/TTCBS
     nil      0)))

(defn- oflags [opts]
  (reduce bit-or HDB/OCREAT
          (list (if (opts :truncate) HDB/OTRUNC  0)
                (if (opts :readonly) HDB/OREADER HDB/OWRITER)
                (if (opts :tsync)    HDB/OTSYNC  0)
                (if (opts :nolock)   HDB/ONOLCK  0)
                (if (opts :noblock)  HDB/OLCKNB  0))))

(defn db-open [opts]
  (let [opts   (merge default-opts opts)
        db     #^HDB (HDB.)
        path   (opts :path)
        bnum   (or (opts :bnum) 0)
        apow   (or (opts :apow) -1)
        fpow   (or (opts :fpow) -1)]
    (when-not (.tune db bnum apow fpow (tflags opts))
      (println path "tune:" (HDB/errmsg (.ecode db))))
    (when-not (.open db path (oflags opts))
      (println path "open:" (HDB/errmsg (.ecode db))))
    (assoc opts :db db)))

(defn db-set [env key val]
  (if (let [db  #^HDB (env :db)
            key #^bytes ((env :key) key)
            val #^bytes ((env :dump) val)]
        (.put db key val))
    val))

(defn db-add [env key val]
  (if (let [db  #^HDB (env :db)
            key #^bytes ((env :key) key)
            val #^bytes ((env :dump) val)]
        (.putkeep db key val))
    val))

(defn db-append [env key val]
  (if (let [db  #^HDB (env :db)
            key #^bytes ((env :key) key)
            val #^bytes ((env :dump) val)]
        (.putcat db key val))
    val))

(defn db-get [env key]
  (let [db  #^HDB (env :db)
        key #^bytes ((env :key) key)  
        val #^bytes (.get db key)]
    (if val ((env :load) val))))

(defn db-update [env key fn]
  (let [db #^HDB (env :db)]
    (.tranbegin db)
    (let [old #^bytes (db-get env key)
          new (fn old)]
      (if (nil? new)
        (.tranabort db)
        (do (db-set env key new)
            (.trancommit db)))
      new)))

(defn db-delete [env key]
  (let [db  #^HDB (env :db)
        key #^bytes ((env :key) key)]
    (.out db key)))
