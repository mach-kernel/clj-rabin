  (defproject clj-rabin "0.1.0-SNAPSHOT"
    :description "FIXME: write description"
    :url "http://example.com/FIXME"
    :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
              :url "https://www.eclipse.org/legal/epl-2.0/"}
    :dependencies [[org.clojure/clojure "1.12.0-alpha6"]]
    :profiles {:dev {:dependencies [[techascent/tech.ml.dataset "7.030"]
                                    [org.clojure/core.async "1.6.681"]
                                    [commons-codec/commons-codec "1.17.1"]]}}
    :repl-options {:init-ns clj-rabin.core})
