(ns clj-rabin.dev.cdc-playground
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clj-rabin.core :as r]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.reductions :as rd])
  (:import (java.io File RandomAccessFile)
           (org.apache.commons.codec.digest DigestUtils)))

(defn file->chunks
  [^File file & [bytes-read]]
  (let [cdc (->> (r/chunk-input-stream (io/input-stream file) :bottom-n 14)
                 (map (fn [[i h]] [(inc i) h]))
                 (cons [0 nil]))
        cdc (concat cdc [[(.length file) nil]])
        raf (RandomAccessFile. file "r")]
    (for [[[start _] [end rabin]] (partition-all 2 1 cdc)
          :when (not (nil? end))
          :let [buf (byte-array end)
                read-len (- end start)
                _ (swap! bytes-read + read-len)]]
                ;_ (.read raf buf start read-len)]]
      {:file   file
       :rabin  rabin
       ;:sha256 (DigestUtils/sha256Hex buf)
       :start  start
       :end    (dec end)
       :size   read-len})))

(defn load-dataset!
  [path ds-atom]
  (let [bytes-read (atom 0)
        file-queue (async/chan)
        chunk-queue (async/chan)
        data (filter File/isFile (file-seq (io/file path)))

        progress-report
        (future
          (let [start (System/currentTimeMillis)]
            (while (nil? @ds-atom)
              (Thread/sleep 1000)
              (println
                (format "Read %d bytes (avg %d bytes/s)"
                        (long @bytes-read)
                        (long (* 1000 (/ @bytes-read (- (System/currentTimeMillis) start)))))))))]
    ; Enqueue dataset files
    (async/onto-chan! file-queue data)
    (async/pipeline
      (-> (Runtime/getRuntime)
          (.availableProcessors))
      chunk-queue
      (mapcat #(file->chunks % bytes-read))
      file-queue)

    ; Drain into TMD dataset
    (reset! ds-atom (ds/->dataset
                      (async/<!! (async/into [] chunk-queue))))

    (future-cancel progress-report)))

(defn chunk-ds->agg-stats
  [ds]
  (rd/aggregate {:total-bytes
                 (rd/sum :size)
                 :total-blocks
                 (rd/row-count)
                 :avg-block-size-bytes
                 (rd/mean :size)}
                ds))

(comment
  (def chunks
    (atom nil))

  (load-dataset! "data/natural_images" chunks)
  (let [rows->long (fn [r] (into {} (map (fn [[k v]] [k (long v)])) r))
        stats-all (chunk-ds->agg-stats @chunks)
        stats-cdc (-> @chunks (ds/unique-by-column :sha256) chunk-ds->agg-stats)
        stats-per-file (->> @chunks
                            (rd/group-by-column-agg :file {:block-count (rd/count-distinct :sha256)})
                            (rd/aggregate {:avg-blocks-per-file (rd/mean :block-count)}))

        ; maps containing aggregate vals
        all (first (map rows->long (ds/rows stats-all)))
        cdc (first (map rows->long (ds/rows stats-cdc)))
        blocks (first (map rows->long (ds/rows stats-per-file)))
        reduced-bytes (- (:total-bytes all)
                         (:total-bytes cdc))]
    {:all all
     :cdc cdc
     :blocks blocks
     :diff {:reduced-bytes reduced-bytes
            :reduced-percent (->> (:total-bytes all)
                                  (/ reduced-bytes)
                                  (* 100)
                                  double)}}))
