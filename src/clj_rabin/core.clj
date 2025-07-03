(ns clj-rabin.core
  (:require [clj-rabin.hash :refer [do-rabin-input-stream ->hash-context]]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(defn chunk-input-stream
  "Emit a sequence of offsets representing content-defined chunks
  within the input stream. Changing the rolling hash parameters
  impacts how many windows are emitted; some inputs emit no chunks.

  opts:
  :mask        Mask for emitting a new chunk
  :buf-size    BufferedInputStream byte[] array size
  :window-size Sliding window size
  :prime       Rabin Polynomial constant
  :q           Modulus"
  [^InputStream is & {:keys [mask] :or {mask 0x1FFF} :as opts}]
  (let [chunks (atom [])]
    (do-rabin-input-stream
      #(if (zero? (bit-and ^long (last %) ^long mask))
         (swap! chunks conj %)
         false)
      is
      (->hash-context opts))
    @chunks))

(comment
  (use 'criterium.core)

  ; ~6mb
  (defn bench-wallpaper
    [])
  (let [file (io/file "/home/mach/Downloads/Fedora-KDE-Live-x86_64-40/fedora.iso")
        then (System/currentTimeMillis)
        _ (chunk-input-stream (io/input-stream file) :buf-size 2000000)
        now (System/currentTimeMillis)]
    (- now then))

  (let [file (io/file "/home/mach/Downloads/Fedora-KDE-Live-x86_64-40/fedora.iso")
        then (System/currentTimeMillis)
        _ (doall (fastcdc-chunk (.toPath file)))
        now (System/currentTimeMillis)]
    (- now then))


  (with-progress-reporting
    (quick-bench
      (bench-wallpaper)))
  (defn fastcdc-chunk
    [^Path p]
    (let [nfr-chunker (-> (ChunkerBuilder.)
                          (.nlFiedlerRust)
                          (.build))]
      (iterator-seq (-> nfr-chunker
                        (.chunk p)
                        (.iterator))))))

(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))

  (time
    (count (chunk-input-stream
             (io/input-stream (io/file "data/enron.tar.gz")))))

  ; CDC a random file
  (let [file (->> (file-seq (io/file "data/maildir"))
                  (filter File/isFile)
                  (rand-nth))]
    [file (chunk-input-stream (io/input-stream file))]))