(ns clj-rabin.core
  (:require [clj-rabin.hash :as hash]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(defn bottom-n-zero?
  [hash n]
  (let [hash (int hash)]
    (-> hash
        (bit-shift-right n)
        (bit-shift-left n)
        (bit-and hash)
        (= hash))))

(defn chunk-input-stream
  "Emit a sequence of offsets representing content-defined chunks
  within the input stream"
  [^InputStream is & {:keys [bottom-n] :or {bottom-n 12}}]
  (filter
    #(bottom-n-zero? (last %) bottom-n)
    (hash/input-stream->hash-seq is {})))

(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))
  (let [file (->> (file-seq (io/file "data/maildir"))
                  (filter File/isFile)
                  (rand-nth))]
    (chunk-input-stream (io/input-stream file))))