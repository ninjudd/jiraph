(ns test.jiraph.tc
  (:use jiraph.tc)
  (:use jiraph.utils)
  (:use test.utils)
  (:use clojure.test))

(use-fixtures :each setup-db-path)

(defn upcase [#^String val]
  (.toUpperCase val))

(deftest tc-raw-access
  (let [db (db-open {:path (str *db-path* "/foo")})]
    (testing "set and get"
             (db-add db 1 "foo")
             (is (= "foo" (db-get db 1)))

             (db-add db 1 "bar") ; should not modify db since key is already there
             (is (= "foo" (db-get db 1)))

             (db-set db 1 "bar")
             (is (= "bar" (db-get db 1)))

             (db-update db 1 upcase)
             (is (= "BAR" (db-get db 1)))

             (db-delete db 1)
             (is (= nil (db-get db 1)))

             )))
