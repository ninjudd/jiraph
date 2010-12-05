(ns jiraph.byte-database-test
  (:refer-clojure :exclude [get count sync])
  (:use clojure.test jiraph.byte-database)
  (:require [jiraph.tokyo-database :as tokyo]))

(deftest byte-database
  (let [db (tokyo/make {:path "/tmp/jiraph-test-tokyo-db" :create true})]
    (open db)
    (truncate! db)    

    (testing "add! doesn't overwrite existing record"
      (is (= nil (get db "foo")))
      (is (= true (add! db "foo" (.getBytes "bar"))))
      (is (= "bar" (String. (get db "foo"))))
      (is (= false (add! db "foo" (.getBytes "baz"))))
      (is (= "bar" (String. (get db "foo")))))

    (testing "put! overwrites existing record"
      (is (= true (put! db "foo" (.getBytes "baz"))))
      (is (= "baz" (String. (get db "foo")))))

    (testing "append! to existing record"
      (is (= true (append! db "foo" (.getBytes "bar"))))
      (is (= "bazbar" (String. (get db "foo"))))
      (is (= true (append! db "foo" (.getBytes "!"))))
      (is (= "bazbar!" (String. (get db "foo")))))

    (testing "append! to nonexistent record"
      (is (= true (append! db "baz" (.getBytes "1234"))))
      (is (= "1234" (String. (get db "baz")))))
    
    (testing "delete! record returns true on success"
      (is (= true (delete! db "foo")))
      (is (= nil (get db "foo")))
      (is (= true (delete! db "baz")))
      (is (= nil (get db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (delete! db "foo")))
      (is (= false (delete! db "bar"))))        

    (testing "len returns -1 for nonexistent records"
      (is (= nil (get db "foo")))
      (is (= -1 (len db "foo"))))

    (testing "len returns the length for existing records"
      (is (= true (put! db "foo" (.getBytes ""))))
      (is (= "" (String. (get db "foo"))))
      (is (= 0 (len db "foo")))
      (is (= true (put! db "bar" (.getBytes "12345"))))
      (is (= 5 (len db "bar")))
      (is (= true (append! db "bar" (.getBytes "6789"))))
      (is (= 9 (len db "bar")))
      (is (= true (add! db "baz" (.getBytes ".........."))))
      (is (= 10 (len db "baz"))))

    (testing "a closed db appears empty"
      (close db)
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))

    (testing "can reopen a closed db"
      (open db)
      (is (not= nil (get db "bar")))
      (is (not= nil (get db "baz")))
      (is (= "123456789" (String. (get db "bar")))))
    
    (testing "truncate deletes all records"
      (is (= true (truncate! db)))
      (is (= nil (get db "foo")))
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))
        
    (close db)))
