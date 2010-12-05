(ns jiraph.byte-database
  (:refer-clojure :exclude [get]))

(defprotocol ByteDatabase "Byte-layer backend database"
  (open       [db])
  (close      [db])
  (sync!      [db])
  (get        [db key])
  (len        [db key])
  (key-seq    [db])
  (add!       [db key val])
  (put!       [db key val])
  (append!    [db key val])
  (inc!       [db key i])
  (delete!    [db key])
  (truncate!  [db])
  (txn-begin  [db])
  (txn-commit [db])
  (txn-abort  [db]))
