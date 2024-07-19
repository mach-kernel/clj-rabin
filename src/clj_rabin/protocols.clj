(ns clj-rabin.protocols
  (:require [clj-rabin.hash :as r])
  (:import [java.io BufferedInputStream InputStream]))

(defprotocol RabinHashable
  (-rabin-hash-seq [this ctx] "Returns a seq of [[]bindex rabin-hash] of this"))

(extend (class (make-array Byte/TYPE 0))
  RabinHashable
  {:-rabin-hash-seq
   (fn [^"[B" this ctx]
     (let [{:keys [window-size prime q buf-size] :as ctx} (merge r/default-ctx ctx)
           buf-size (or buf-size (alength this))
           window-size (if (>= window-size (alength this))
                         (dec buf-size)
                         window-size)
           pow (r/window-pow ctx)
           ;; NOTE: reductions emits the initial value, so we do not have to cons
           ;; the first window's hash and index to the list
           hashes (reductions
                   (fn [acc i]
                     (let [out-byte (nth this (- i window-size))
                           in-byte (nth this i)]
                       (-> acc
                           (* prime)
                           (+ in-byte)
                           (- (* out-byte pow))
                           (mod q))))
                   (r/poly-hash ctx this)
                   (range window-size buf-size))]
       ;; ...and that is why we dec here
       (->> hashes
            (interleave (range (dec window-size) buf-size))
            (partition-all 2))))})

(extend BufferedInputStream
  RabinHashable
  {:-rabin-hash-seq
   (fn [^BufferedInputStream this {:keys [buf-size pos] :or {pos 0 buf-size 1e6} :as ctx}]
     (when (pos? (.available this))
       (lazy-seq
        (let [buf (byte-array buf-size)
              bytes-read (.read this buf 0 buf-size)]
          (concat (map (fn [[i h]] [(+ pos i) h])
                       (-rabin-hash-seq buf (assoc r/default-ctx :buf-size bytes-read)))
                  (-rabin-hash-seq this (update ctx :pos + bytes-read)))))))})

(extend InputStream
  RabinHashable
  {:-rabin-hash-seq
   (fn [^InputStream this ctx]
     (-rabin-hash-seq (BufferedInputStream. this) ctx))})
