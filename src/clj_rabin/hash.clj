(ns clj-rabin.hash
  (:require [clojure.java.io :as io])
  (:import (java.io BufferedInputStream InputStream)))

(def default-ctx
  "prime              : should be close to the alphabet size
   q (modulus)        : should be sufficiently large to avoid collisions, also prime
   window-size (bytes): can be anything (but as little as 16 bytes is 'enough')"
  {:prime       257
   :q           153191                                      ;Integer/MAX_VALUE
   :window-size 16})

(defn bigint-pow
  [a b]
  (reduce * (repeat b (bigint a))))

(defn lsb-zero?
  "Are the bottom n bits 0?"
  [hash n]
  (let [hash (int hash)]
    (-> hash
        (bit-shift-right n)
        (bit-shift-left n)
        (bit-and hash)
        (= hash))))

(defn window-pow
  "pow = p^window-sz % q"
  [{:keys [window-size prime q]}]
  (-> (bigint-pow prime window-size)
      (mod q)))

(defn poly-hash
  "Compute the polynomial hash for a window
  P^w*a[i] + P^w-1[i+1] + ..."
  [{:keys [window-size prime q]} ^bytes bs]
  (let [hash (reduce (fn [acc i]
                       (-> (bigint-pow prime (- (dec window-size) i))
                           (* (nth bs i))
                           (+ acc)))
                     0
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
   (let [{:keys [window-size prime q buf-size] :as ctx} (merge default-ctx ctx)
         buf-size (or buf-size (alength bs))
         window-size (if (>= window-size (alength bs))
                       (dec buf-size)
                       window-size)
         pow (window-pow ctx)]
     ; NOTE: reductions emits the first 'init' window too
     (reductions
       (fn [[_ acc] i]
         (let [out-byte (nth bs (- i window-size))
               in-byte (nth bs i)]
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
        {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)
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
  ([^BufferedInputStream bis ^Long pos {:keys [buf-size] :or {buf-size 1000000} :as opts}]
   (when (pos? (.available bis))
     (lazy-seq
       (let [buf (byte-array buf-size)
             bytes-read (.read bis buf 0 buf-size)]
         (concat (->> buf
                      (byte-array->hash-seq (assoc default-ctx :buf-size bytes-read))
                      (map (fn [[i h]]
                             [(+ pos i) h])))
                 (input-stream->hash-seq bis (+ pos bytes-read) opts)))))))
