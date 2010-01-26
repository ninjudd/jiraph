(use 'clojure.test)

(def test-names [:tc :bdb :graph])
 
(def test-namespaces
     (map #(symbol (str "test.jiraph." (name %)))
          test-names))
 
(defn run []
  (println "Loading tests...")
  (apply require :reload-all test-namespaces)
  (apply run-tests test-namespaces))

(run)