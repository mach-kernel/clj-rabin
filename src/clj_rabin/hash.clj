(ns clj-rabin.hash
  (:require [clojure.math :as math])
  (:import (java.io BufferedInputStream InputStream)
           (java.util Arrays)))

(def default-ctx
  "prime              : should be close to the alphabet size
   q (modulus)        : should be sufficiently large to avoid collisions, also prime
   window-size (bytes): can be anything (but as little as 16 bytes is 'enough')"
  {:prime       257
   :q           (Integer/MAX_VALUE)
   :window-size 32})

(defn window-pow
  "pow = p^window-sz % q"
  [{:keys [window-size prime q]}]
  (-> (math/pow prime window-size)
      (mod q)))

(defn poly-hash
  "Compute the polynomial hash for a window
  P^w*a[i] + P^w-1[i+1] + ..."
  [{:keys [window-size prime q]} ^bytes bs]
  (let [hash (reduce (fn [acc i]
                       (-> (math/pow prime (- (dec window-size) i))
                           (* (nth bs i))
                           (+ acc)))
                     0
                     (range window-size))]
    (mod hash q)))

(defn byte-array->hash-seq
  "Given a rabin context and some bytes, emit a seq of [[index rabin-hash] ...]
  each index beginning from the end of the first window"
  ([^bytes bs]
   (byte-array->hash-seq default-ctx bs))
  ([{:keys [window-size prime q] :as ctx} ^bytes bs]
   (let [window-size (if (>= window-size (alength bs))
                       (dec (alength bs))
                       window-size)
         pow (window-pow ctx)
         ; NOTE: reductions emits the initial value, so we do not have to cons
         ; the first window's hash and index to the list
         hashes (reductions
                  (fn [acc i]
                    (let [out-byte (nth bs (- i window-size))
                          in-byte (nth bs i)]
                      (-> (* acc prime)
                          (+ in-byte)
                          (- (* out-byte pow))
                          (mod q))))
                  (poly-hash ctx bs)
                  (range window-size (alength bs)))]
     ; ...and that is why we dec here
     (->> (interleave (range (dec window-size) (alength bs)) hashes)
          (partition-all 2)))))

(defn input-stream->hash-seq
  "Given an arbitrarily large sequence, emit a sequence of rabin hashes at
  each index beginning from the end of the first window"
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
         (concat (->> (Arrays/copyOfRange buf 0 bytes-read)
                   (byte-array->hash-seq)
                   (map (fn [[i h]]
                          [(+ pos i) h])))
                 (input-stream->hash-seq bis (+ pos bytes-read) opts)))))))

(comment
  ; find common windows in O(n)
  (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
        {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)
        hash-seq (byte-array->hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h (Integer/toBinaryString h) (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))