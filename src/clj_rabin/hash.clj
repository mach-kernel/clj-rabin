(ns clj-rabin.hash
  (:require [clojure.java.io :as io])
  (:import (java.io BufferedInputStream InputStream)))

(def default-ctx
  "prime              : should be close to the alphabet size
   q (modulus)        : should be sufficiently large to avoid collisions, also prime
   window-size (bytes): can be anything (but as little as 16 bytes is 'enough')"
  {:prime       (long 257)
   :q           (long 153191)                                      ;Integer/MAX_VALUE
   :window-size (int 16)})

(defn long-pow
  ^long
  [^long a ^long b]
  (reduce unchecked-multiply 1 (repeat b a)))

(defn lsb-zero?
  "Are the bottom n bits 0?"
  [^long hash ^Integer n]
  (-> hash
      (bit-shift-right ^long n)
      (bit-shift-left ^long n)
      (bit-and ^long hash)
      (= hash)))

(defn window-pow
  "pow = p^window-sz % q"
  ^long
  [{:keys [window-size ^long prime ^long q]}]
  (-> (long-pow prime window-size)
      (mod q)))

(defn poly-hash
  "Compute the polynomial hash for a window
  P^w*a[i] + P^w-1[i+1] + ..."
  ^long
  [{:keys [^long window-size ^long prime ^long q]} ^bytes bs]
  (let [hash (reduce (fn [^long acc ^long i]
                       (-> (long-pow prime (- (dec window-size) i))
                           (* ^byte (nth bs i))
                           (+ acc)))
                     (long 0)
                     (range window-size))]
    (mod hash q)))

(defn byte-array->hash-seq
  "Given a rabin context and some bytes, emit a seq of [[index rabin-hash] ...]
  each index beginning from the end of the first window

  opts:

  :window-size Sliding window size
  :prime       Rabin Polynomial constant
  :q           Modulus"
  ([^bytes bs]
   (byte-array->hash-seq default-ctx bs))
  ([ctx ^bytes bs]
   (let [{:keys [^long window-size ^long prime ^long q ^Integer buf-size] :as ctx} (merge default-ctx ctx)
         ^long buf-size (or buf-size (alength bs))
         ^long window-size (if (>= window-size (long (alength bs)))
                             (dec buf-size)
                             window-size)
         pow (window-pow ctx)]
     ; NOTE: reductions emits the first 'init' window too
     (reductions
       (fn [[_ ^long acc] ^long i]
         (let [^byte out-byte (nth bs (- i window-size))
               ^byte in-byte (nth bs i)]
           [i (-> (* acc prime)
                  (+ in-byte)
                  (- (* out-byte pow))
                  (mod q))]))
       ; the first window starts at len(window_sz) - 1
       [(dec window-size) (poly-hash ctx bs)]
       (range window-size buf-size)))))

(comment

  (use 'criterium.core)

  (with-progress-reporting
    (quick-bench
      (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
            {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)]
        (doall (byte-array->hash-seq rabin-ctx some-data)))))

  (dotimes [_ 10000]
    (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
          {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)]
      (doall (byte-array->hash-seq rabin-ctx some-data))))

  ; find repeating sequences of an arbitrary window size
  (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
        {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 3)
        hash-seq (byte-array->hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h
            (Integer/toBinaryString h)
            (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))

(defn input-stream->hash-seq
  "Given an arbitrarily large sequence, emit a sequence of rabin hashes at
  each index beginning from the end of the first window

  Accepts Rabin opts and:
  :buf-size BufferedInputStream byte[] array size"
  ([^InputStream input-stream]
   (input-stream->hash-seq input-stream {}))
  ([^InputStream input-stream opts]
   (input-stream->hash-seq
     (if (instance? BufferedInputStream input-stream)
       input-stream
       (BufferedInputStream. input-stream))
     0
     opts))
  ([^BufferedInputStream bis ^long pos {:keys [buf-size] :or {buf-size 1000000} :as opts}]
   (when (pos? (.available bis))
     (lazy-seq
       (let [buf (byte-array buf-size)
             bytes-read (.read bis buf 0 buf-size)]
         (concat (->> buf
                      (byte-array->hash-seq (assoc default-ctx :buf-size bytes-read))
                      (map (fn [[i h]]
                             [(+ pos i) h])))
                 (input-stream->hash-seq bis (+ pos bytes-read) opts)))))))
