(ns clj-rabin.hash
  (:require [clojure.java.io :as io])
  (:import (clojure.lang PersistentVector)
           (java.io BufferedInputStream InputStream)))

(def default-ctx
  "prime              : should be close to the alphabet size
   q (modulus)        : should be sufficiently large to avoid collisions, also prime
   window-size (bytes): can be anything (but as little as 16 bytes is 'enough')"
  {:prime          (long 257)
   :q              (long 153191)                            ;Integer/MAX_VALUE
   :window-size    (int 32)
   :min-chunk-size (int 3000)})

(defn mod-pow
  ^long
  [^long a ^long b ^long q]
  (reduce (fn [^long a ^long b]
            (mod ^long (* a b) q)) 1 (repeat b a)))

(def range-vec
  (memoize (fn ^PersistentVector
             [s & [e]]
             (into [] (if e
                        (range s e)
                        (range s))))))

(defn lsb-zero?
  "Are the bottom n bits 0?"
  [^long hash ^Integer n]
  (-> hash
      (unsigned-bit-shift-right ^long n)
      (bit-shift-left ^long n)
      (bit-and ^long hash)
      (= hash)))

(defn ->pow-byte-table
  "Returns a vector of (pow * out-byte % q) for 00..FF"
  [^long pow ^long q]
  (mapv
    (fn [b]
      ^long (mod (* ^long pow ^long b) ^long q))
    (range 0 256)))

(defn ->hash-context
  "Given a bag of Rabin parameter overrides, prepare a hashing context"
  [ctx]
  (let [{:keys [^long window-size ^long prime ^long q] :as ctx} (merge default-ctx ctx)
        pow (mod-pow prime window-size q)
        pow-table (->pow-byte-table pow q)]
    (assoc ctx :pow pow
               :pow-table pow-table)))

(defn poly-hash
  "Compute the polynomial hash for a window
  P^w*a[i] + P^w-1[i+1] + ..."
  ^long
  [{:keys [^long window-size ^long prime ^long q]} ^bytes bs]
  (let [hash (reduce (fn [^long acc ^long i]
                       (-> (mod-pow prime (- (dec window-size) i) q)
                           (* ^byte (nth bs i))
                           (+ acc)))
                     (long 0)
                     (range-vec window-size))]
    (mod hash q)))

(defn slide-roll-hash
  "Increment the hash given the byte entering the window (in-byte)
  and the byte leaving the window (out-byte)"
  ^long
  [{:keys [^long prime ^long q pow-table]} ^long prev-hash out-byte in-byte]
  (-> (* prev-hash prime)
      (+ ^byte in-byte)
      ; (- (* ^byte out-byte ^long pow))
      ; java signed byte lookup against 0-255
      (- ^long (nth pow-table (bit-and ^byte out-byte 0xFF)))
      (mod q)))

(defn do-rabin
  "Given a function (for side effects), a rabin context, and some bytes, emit a seq of
  [[index rabin-hash] ...] each index beginning from the end of the first window. If f
  returns true, min-window-size is skipped.

  pow (computed for you) = p^window-sz % q

  opts:

  :window-size Sliding window size
  :prime       Rabin Polynomial constant
  :q           Modulus"
  ([f ^bytes bs]
   (do-rabin f default-ctx bs))
  ([f {:keys [^long window-size ^Integer buf-size ^Integer min-chunk-size] :as ctx} ^bytes bs]
   (let [^long buf-size (or buf-size (alength bs))
         ^long window-size (if (>= window-size (long (alength bs)))
                             (dec buf-size)
                             window-size)
         start-hash (poly-hash ctx bs)
         start (dec window-size)]
     (when-not (f start start-hash)
       (loop [i (inc start)
              prev-hash start-hash]
         (when (< i buf-size)
           (let [roll-hash (slide-roll-hash
                             ctx
                             prev-hash
                             ; out-byte
                             ^byte (nth bs (- i window-size))
                             ; in-byte
                             ^byte (nth bs i))]
             (recur (if (f i roll-hash)
                      (+ ^long i ^long min-chunk-size)
                      (inc i)) ^long roll-hash))))))))

(defn byte-array->hash-seq
  "Given a rabin context and some bytes, emit a seq of [[index rabin-hash] ...]
  each index beginning from the end of the first window

  pow (computed for you) = p^window-sz % q

  opts:

  :window-size Sliding window size
  :prime       Rabin Polynomial constant
  :q           Modulus"
  ([^bytes bs]
   (byte-array->hash-seq default-ctx bs))
  ([{:keys [^long window-size ^Integer buf-size] :as ctx} ^bytes bs]
   (let [^long buf-size (or buf-size (alength bs))
         ^long window-size (if (>= window-size (long (alength bs)))
                             (dec buf-size)
                             window-size)]
     ; NOTE: reductions emits the first 'init' window too
     (reductions
       (fn [[_ ^long acc] ^long i]
         (let [^byte out-byte (nth bs (- i window-size))
               ^byte in-byte (nth bs i)]
           [i (slide-roll-hash ctx acc out-byte in-byte)]))
       ; the first window starts at a[len(window_sz) - 1]
       [(dec window-size) (poly-hash ctx bs)]
       (range-vec window-size buf-size)))))

(defn do-rabin-input-stream
  "Given an arbitrarily large sequence, emit a sequence of rabin hashes at
  each index beginning from the end of the first window

  Accepts Rabin opts and:
  :buf-size BufferedInputStream byte[] array size"
  ([f ^InputStream input-stream]
   (do-rabin-input-stream f input-stream {}))
  ([f ^BufferedInputStream input-stream {:keys [buf-size] :or {buf-size 1000000} :as ctx}]
   (let [ctx (->hash-context ctx)
         bis (if (instance? BufferedInputStream input-stream)
               input-stream
               (BufferedInputStream. input-stream))
         pos (atom (long 0))
         offset-i (fn [^long i ^long h] [(+ ^long @pos ^long i) h])]
     (while (pos? (.available bis))
       (let [buf (byte-array buf-size)
             bytes-read (.read bis buf 0 buf-size)]
         (do-rabin (comp f offset-i) (assoc ctx :buf-size bytes-read) buf)
         (swap! pos + bytes-read))))))

(defn input-stream->hash-seq
  "Given an arbitrarily large sequence, emit a sequence of rabin hashes at
  each index beginning from the end of the first window.

  Accepts Rabin opts and:
  :buf-size BufferedInputStream byte[] array size"
  ([^InputStream input-stream]
   (input-stream->hash-seq input-stream {}))
  ([^InputStream input-stream ctx]
   (input-stream->hash-seq
     (if (instance? BufferedInputStream input-stream)
       input-stream
       (BufferedInputStream. input-stream))
     0
     ctx))
  ([^BufferedInputStream bis ^long pos {:keys [buf-size] :or {buf-size 1000000} :as ctx}]
   (let [ctx (->hash-context ctx)]
     (when (pos? (.available bis))
       (lazy-seq
         (let [buf (byte-array buf-size)
               bytes-read (.read bis buf 0 buf-size)]
           (concat (->> buf
                        (byte-array->hash-seq (assoc ctx :buf-size bytes-read))
                        (map (fn [[^long i h]]
                               [(+ pos i) h])))
                   (input-stream->hash-seq bis (+ pos bytes-read) ctx))))))))

(comment
  (use 'criterium.core)

  (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
        {:keys [window-size] :as rabin-ctx} (->hash-context {:window-size 4})]
    (with-progress-reporting
      (quick-bench
        (do-rabin (fn [a b]) rabin-ctx some-data))))

  (dotimes [_ 1000000]
    (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
          {:keys [window-size] :as rabin-ctx} (->hash-context {:window-size 10})]
      (doall (do-rabin (fn [a b]) default-ctx some-data))))

  ; find repeating sequences of an arbitrary window size
  (let [some-data (.getBytes "AAAAACCCCCAAAAACCCCCCAAAAAGGGTTT")
        {:keys [window-size] :as rabin-ctx} (->hash-context {:window-size 10})
        hash-seq (byte-array->hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h
            (Long/toBinaryString h)
            (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))