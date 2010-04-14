(ns jiraph.tc
  (:use jiraph.utils)
  (:import tokyocabinet.HDB))

(set! *warn-on-reflection* true)

(def in-transaction? false)

(def default-opts
  {:key  #(.getBytes (str %))
   :dump #(.getBytes (pr-str %))
   :load read-append-bytes})

(defn- tflags [opts]
  (bit-or
   (if (opts :large) HDB/TLARGE 0)
   (condp = (opts :compress)
     :deflate HDB/TDEFLATE
     :bzip    HDB/TBZIP
     :tcbs    HDB/TTCBS
     nil      0)))

(defn- oflags [opts]
  (reduce bit-or 0
          (list (if (opts :create)   HDB/OCREAT  0)
                (if (opts :truncate) HDB/OTRUNC  0)
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
    (with-meta
      (assoc opts :db db)
      {:type ::layer})))

(defmacro db-send [action layer & args]
  (condp = (count args)
    0 `(let [db# #^HDB (~layer :db)]
         (. db# ~action))
    1 (let [[key] args]
        `(let [db#  #^HDB (~layer :db)
               key# (bytes ((~layer :key) ~key))]
         (. db# (~action key#))))
    2 (let [[key val] args]
        `(when (let [db#  #^HDB (~layer :db)
                     key# (bytes ((~layer :key)  ~key))
                     val# (bytes ((~layer :dump) ~val))]
                 (. db# (~action key# val#)))
           ~val))))

(defn db-set [layer key val]
  (db-send put layer key val))

(defn db-add [layer key val]
  (db-send putkeep layer key val))

(defn db-append [layer key val]
  (db-send putcat layer key val))

(defn db-len [layer key]
  (db-send vsiz layer key))

(defn db-get [layer key]
  (let [val #^bytes (db-send get layer key)]
    (if val ((layer :load) val))))

(defn db-get-slice [layer key length]
  (let [val #^bytes (db-send get layer key)]
    (if val ((layer :load) val length))))

(defn db-delete [layer key]
  (db-send out layer key))

(defmacro db-transaction [layer & body]
  `(if in-transaction?
     (do ~@body)
     (binding [in-transaction? true]
       (let [db# #^HDB (~layer :db)]
         (.tranbegin db#)
         (if-let [result# (do ~@body)]
           (do (.trancommit db#)
               result#)
           (.tranabort db#))))))

(defn db-truncate [layer]
  (db-send vanish layer))

(defn db-count [layer]
  (db-send rnum layer))

(defn db-close [layer]
  (db-send close layer))

(defn db-get-meta [layer]
  (let [db  #^HDB (layer :db)
        key #^bytes (.getBytes "_meta")
        val #^bytes (. db (get key))]
    (if val (read-string (String. val)) {})))

(defn db-set-meta [layer meta]
  (let [db  #^HDB (layer :db)
        key #^bytes (.getBytes "_meta")
        val #^bytes (.getBytes (pr-str meta))]
    (when (. db (put key val))
      meta)))
