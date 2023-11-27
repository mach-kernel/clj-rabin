(ns clj-rabin.core)

; alt; openssl prime -generate -bits n
; https://github.com/moinakg/pcompress/blob/c6e779c40041b7bb46259e9806fa92b20c7b78fb/rabin/rabin_dedup.h#L76
(def RAB-PRIME
  153191)

(def RAB-MOD
  101)

(defn with-outbyte
  "Take a partitioned seq of bytes and weave the byte leaving
  the sliding window at each step, e.g.
  [1 2 3 4 5] with window size 2:
  [[nil [1 2]] [1 [2 3]] ...]"
  [out-byte partitioned-bytes]
  (lazy-seq
    (when-let [[p & ps] partitioned-bytes]
      (cons [out-byte p] (with-outbyte (first p) ps)))))

(defn windowed-with-outbyte
  "Partition bytes into window-sz interleaved with the previous
  window's out-byte"
  [^Integer window-sz ^bytes bs]
  (->> bs
       (partition window-sz 1 [0])
       (with-outbyte 0)))

(defn rabin-hash
  "Given pow, a Rabin constant, in/out bytes, and an optional previous value,
  compute the next Rabin hash"
  [{:keys [pow rab-p rab-m]} out-byte in-byte & [prev-hash]]
  (let [prev-hash (or prev-hash 0)
        new-hash (- (+ (* prev-hash rab-p)
                       in-byte)
                    (* out-byte pow))]
    ; i.e. avoid overflows
    (mod new-hash rab-m)))

(defn rolling-hash-seq
  "Given a window-size, Rabin polynomial constant, modulus, and windowed
  array bufs, emit a sequence of Rabin hashes"
  ([{:keys [window-size rab-p rab-m] :as ctx} windows]
   (rolling-hash-seq
     (assoc ctx :pow (mod (Math/pow rab-p window-size)
                          rab-m))
     0
     windows))
  ([ctx prev-hash windows]
   (lazy-seq
     (when (and ctx windows)
       (let [[[out-byte window] & r] windows
             next-hash (rabin-hash
                         ctx out-byte (last window) prev-hash)]
         (cons next-hash (rolling-hash-seq ctx next-hash r)))))))

(comment
  ; find common substrings in O(n)
  (let [some-data "abcdefghabcdefzz5"
        rabin-ctx {:window-size 3 :rab-p RAB-PRIME :rab-m RAB-MOD}
        windowed (windowed-with-outbyte (:window-size rabin-ctx)
                                        (.getBytes some-data))
        hash-seq (rolling-hash-seq rabin-ctx windowed)
        char-seq (->> (.getBytes some-data)
                      (partition (:window-size rabin-ctx) 1 [0])
                      (map #(String. (byte-array %))))]
    (->> (interleave hash-seq char-seq)
         (partition-all 2)
         (group-by first))))
