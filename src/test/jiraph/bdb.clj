(ns test.jiraph.bdb
  (:use jiraph.bdb)
  (:use test.utils)
  (:use clojure.test))
 
(use-fixtures :each setup-db-path)

(defn db-get-str [env table key]
  (let [val #^bytes (db-get env table key)]
    (String. val)))

(defn upcase [#^bytes val]
  (.toUpperCase (String. val)))

(deftest bdb-raw-access
  (let [db (db-open *db-path*)]
    (testing "set and get"
             (db-add db :nodes 1 "foo")
             (is (= "foo" (db-get-str db :nodes 1)))

             (db-add db :nodes 1 "bar") ; should not modify db since key is already there
             (is (= "foo" (db-get-str db :nodes 1)))

             (db-set db :nodes 1 "bar")
             (is (= "bar" (db-get-str db :nodes 1)))

             (db-update db :nodes 1 upcase)
             (is (= "BAR" (db-get-str db :nodes 1)))

             (db-delete db :nodes 1)
             (is (= nil (db-get db :nodes 1)))

             )))
