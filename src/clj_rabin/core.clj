(ns clj-rabin.core
  (:require [clj-rabin.hash :refer [lsb-zero? do-rabin-input-stream]]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(defn chunk-input-stream
  "Emit a sequence of offsets representing content-defined chunks
  within the input stream. Changing the rolling hash parameters
  impacts how many windows are emitted; some inputs emit no chunks.

  opts:
  :bottom-n LSB mask size for emitting a new chunk
  :buf-size BufferedInputStream byte[] array size
  :window-size Sliding window size
  :prime       Rabin Polynomial constant
  :q           Modulus"
  [^InputStream is & {:keys [bottom-n] :or {bottom-n 12} :as opts}]
  (let [chunks (atom [])]
    (do-rabin-input-stream
      #(when (lsb-zero? (last %) bottom-n)
         (swap! chunks conj %))
      is
      opts)
    @chunks))

(comment
  (use 'criterium.core)

  ; ~6mb
  (defn bench-wallpaper
    []
    (let [file (io/file "/home/mach/Pictures/Wallpapers/04086_queenstownfrombobspeak_3840x2400.jpg")]
      (doall (chunk-input-stream (io/input-stream file) :buf-size 2000000))))

  (with-progress-reporting
    (quick-bench
      (bench-wallpaper))))


(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))

  (count (chunk-input-stream
           (io/input-stream (io/file "data/enron.tar.gz"))
           :bottom-n 14))

  ; CDC a random file
  (let [file (->> (file-seq (io/file "data/maildir"))
                  (filter File/isFile)
                  (rand-nth))]
    (chunk-input-stream (io/input-stream file))))