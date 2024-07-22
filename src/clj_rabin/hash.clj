(ns clj-rabin.hash
  (:require [clojure.java.io :as io])
  (:import (clojure.lang PersistentVector)
           (java.io BufferedInputStream InputStream)))

(def default-ctx
  "prime              : should be close to the alphabet size
   q (modulus)        : should be sufficiently large to avoid collisions, also prime
   window-size (bytes): can be anything (but as little as 16 bytes is 'enough')"
  {:prime       (long 257)
   :q           (long 153191)                               ;Integer/MAX_VALUE
   :window-size (int 16)})

(defn mod-pow
  ^long
  [^long a ^long b ^long q]
  (reduce (fn [a b]
            (mod (* a b) q)) 1 (repeat b a)))

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

(defn window-pow
  "pow = p^window-sz % q"
  ^long
  [{:keys [window-size ^long prime ^long q]}]
  (mod-pow prime window-size q))

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
  [{:keys [^long prime ^long pow ^long q]} ^long prev-hash out-byte in-byte]
  (-> (* prev-hash prime)
      (+ ^byte in-byte)
      (- (* ^byte out-byte ^long pow))
      (mod q)))

(defn do-rabin
  ([f ^bytes bs]
   (do-rabin f default-ctx bs))
  ([f ctx ^bytes bs]
   (let [{:keys [^long window-size ^Integer buf-size] :as ctx} (merge default-ctx ctx)
         ctx (assoc ctx :pow (window-pow ctx))
         ^long buf-size (or buf-size (alength bs))
         ^long window-size (if (>= window-size (long (alength bs)))
                             (dec buf-size)
                             window-size)
         start-hash (poly-hash ctx bs)
         start (dec window-size)]
     (f start start-hash)
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
           (f i roll-hash)
           (recur (inc i) ^long roll-hash)))))))

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
   (let [{:keys [^long window-size ^Integer buf-size] :as ctx} (merge default-ctx ctx)
         ctx (assoc ctx :pow (window-pow ctx))
         ^long buf-size (or buf-size (alength bs))
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
  ([f ^BufferedInputStream input-stream {:keys [buf-size] :or {buf-size 1000000} :as opts}]
   (let [bis (if (instance? BufferedInputStream input-stream)
               input-stream
               (BufferedInputStream. input-stream))
         pos (atom (long 0))
         offset-i (fn [^long i ^long h] [(+ ^long @pos ^long i) h])]
     (while (pos? (.available bis))
       (let [buf (byte-array buf-size)
             bytes-read (.read bis buf 0 buf-size)]
         (do-rabin (comp f offset-i) (assoc opts :buf-size bytes-read) buf)
         (swap! pos + bytes-read))))))

(defn input-stream->hash-seq
  "Given an arbitrarily large sequence, emit a sequence of rabin hashes at
  each index beginning from the end of the first window.

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
                      (byte-array->hash-seq (assoc opts :buf-size bytes-read))
                      (map (fn [[i h]]
                             [(+ pos i) h])))
                 (input-stream->hash-seq bis (+ pos bytes-read) opts)))))))

(comment
  (use 'criterium.core)

  (with-progress-reporting
    (quick-bench
      (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
            {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)]
        (do-rabin (fn [a b]) rabin-ctx some-data))))

  (let [file (io/file "/home/mach/Pictures/Wallpapers/04086_queenstownfrombobspeak_3840x2400.jpg")
        whole-file (io/input-stream file)
        wf-bs (.readAllBytes whole-file)]
    #_(doall (byte-array->hash-seq default-ctx wf-bs))
    (do-rabin #(do [%1 %2]) default-ctx wf-bs)
    nil)

  (dotimes [_ 1000000]
    (let [some-data (.getBytes "abcdefghabcdefzabcdz54325aadgfsfgabcd")
          {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 4)]
      (doall (do-rabin (fn [a b]) default-ctx some-data))))

  ; find repeating sequences of an arbitrary window size
  (let [some-data (.getBytes "AAAAACCCCCAAAAACCCCCCAAAAAGGGTTT")
        {:keys [window-size] :as rabin-ctx} (assoc default-ctx :window-size 10)
        hash-seq (byte-array->hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h
            (Long/toBinaryString h)
            (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))