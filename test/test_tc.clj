(ns test-tc
  (:use jiraph.tc)
  (:use jiraph.utils)
  (:use clojure.test))

(def db (db-init {:path "/tmp/jiraph-test-foo" :create true}))

(defn clear-db [f]
  (map db-truncate db)
  (f))

(use-fixtures :each clear-db)

(deftest tc-raw-access
  (testing "set and get"
    (db-open db)
    (db-add db 1 "foo")
    (is (= "foo" (db-get db 1)))

    (db-add db 1 "bar") ; should not modify db since key is already there
    (is (= "foo" (db-get db 1)))

    (db-set db 1 "bar")
    (is (= "bar" (db-get db 1)))

    (db-delete db 1)
    (is (= nil (db-get db 1)))
    (db-close db)
    ))
