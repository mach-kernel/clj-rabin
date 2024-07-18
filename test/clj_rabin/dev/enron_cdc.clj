(ns clj-rabin.dev.enron-cdc
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clj-rabin.core :as r]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.reductions :as rd])
  (:import (java.io File RandomAccessFile)
           (java.util UUID)
           (org.apache.commons.codec.digest DigestUtils)))

(def file-queue
  (async/chan))

(def chunk-queue
  (async/chan))

(def status-queue
  (async/chan
    (async/dropping-buffer 1000)))

(def chunks
  (atom nil))

(defn file->chunks
  [^File file]
  (let [cdc (->> (r/chunk-input-stream (io/input-stream file) :bottom-n 10)
                 (map (fn [[i h]] [(inc i) h]))
                 (cons [0 nil]))
        cdc (concat cdc [[(.length file) nil]])
        raf (RandomAccessFile. file "r")]
    (for [[[start _] [end rabin]] (partition-all 2 1 cdc)
          :when (not (nil? end))
          :let [buf (byte-array end)
                read-len (- end start)
                _ (async/put! status-queue {:size read-len :t (System/currentTimeMillis)})
                _ (.read raf buf start read-len)]]
      {:id     (UUID/randomUUID)
       :file   file
       :rabin  rabin
       :sha256 (DigestUtils/sha256Hex buf)
       :start  start
       :end    (dec end)
       :size   read-len})))

(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))

  ; Apply CDC to all Enron data
  (let [mail (filter File/isFile (file-seq (io/file "data/maildir")))]
    (async/onto-chan! file-queue mail)
    (async/pipeline
      (-> (Runtime/getRuntime)
          (.availableProcessors))
      chunk-queue
      (mapcat file->chunks)
      file-queue))

  ; Status output
  (letfn [(take-status []
            (->> status-queue
                 (async/take 100)
                 (async/into [])
                 (async/<!!)))]
    (async/go-loop [blocks (take-status)]
      (let [bs (reduce + (map :size blocks))
            ts (- (:t (last blocks))
                  (:t (first blocks)))]
        (println
          (format "Chunked %d bytes (avg %d bytes/sec)"
                  (biginteger bs)
                  (biginteger (* 1000 (/ bs ts)))))
        (async/<! (async/timeout 1000))
        (when blocks
          (recur (take-status))))))

  ; Drain into a dataset
  (reset! chunks (ds/->dataset
                   (async/<!! (async/into [] chunk-queue))))

  ; Present findings
  (let [uniques (-> @chunks (ds/unique-by-column :sha256))
        corpus-size-bytes (->> @chunks
                               (rd/aggregate {:total-bytes (rd/sum :size)})
                               ds/rows
                               first
                               :total-bytes)
        cdc-size-bytes (->> uniques
                            (rd/aggregate {:total-bytes (rd/sum :size)})
                            ds/rows
                            first
                            :total-bytes)
        reduced-by (long (- corpus-size-bytes cdc-size-bytes))]
    {:corpus-size-bytes (long corpus-size-bytes)
     :cdc-size-bytes    (long cdc-size-bytes)
     :reduced-by-bytes  reduced-by
     :reduced-by-pct (* 100 (/ reduced-by corpus-size-bytes))
     :total-blocks      (ds/row-count @chunks)
     :cdc-total-blocks  (ds/row-count uniques)}))
