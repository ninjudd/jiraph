(ns jiraph.byte-database
  (:refer-clojure :exclude [get sync]))

(defprotocol ByteDatabase "Byte-layer backend database"
  (open      [db])
  (close     [db])
  (sync      [db])
  (get       [db key])
  (len       [db key])
  (txn       [db f])
  (add!      [db key val])
  (put!      [db key val])
  (append!   [db key val])
  (inc!      [db key i])
  (delete!   [db key])
  (truncate! [db]))
