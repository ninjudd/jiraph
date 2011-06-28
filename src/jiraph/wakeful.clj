(ns jiraph.wakeful
  (:use ring.adapter.jetty wakeful.core
        ring.middleware.stacktrace))

(def handler (wrap-stacktrace (wakeful "jiraph" :content-type "text/html")))

(defn run [] (def j (run-jetty #'handler {:port 8080 :join? false})))
