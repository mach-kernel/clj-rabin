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
  [[nil [1 2]] [1 [2 3] ...]"
  [out-byte partitioned-bytes]
  (lazy-seq
    (when-let [[p & ps] partitioned-bytes]
      (cons [out-byte p] (with-outbyte (first p) ps)))))

(defn windowed-with-outbyte
  "Partition bytes into window-sz interleaved with the previous
  window's out-byte"
  [^Integer window-sz ^bytes bs]
  (->> bs
       (partition window-sz 1 [:pad])
       (with-outbyte nil)))

(defn rolling-hash-seq
  "Given a window-size, Rabin polynomial constant, modulus, and windowed
  array bufs, emit a sequence of Rabin hashes"
  ([win-sz rab-p rab-m win-bufs]
   (let [pow (mod (Math/pow rab-p win-sz) rab-m)]
     (rolling-hash-seq 0 pow rab-p rab-m win-bufs)))
  ([rolling-hash pow rab-p rab-m win-bufs]
   (lazy-seq
     (when-let [[[out-byte win] & r] win-bufs]
       (let [in-byte (if (number? (last win))
                       (last win)
                       0)
             new-hash (- (+ (* rolling-hash rab-p)
                            in-byte)
                         (* (if (number? out-byte)
                              out-byte
                              0) pow))
             ; i.e. avoid overflows
             new-hash (mod new-hash rab-m)]
         (cons new-hash (rolling-hash-seq new-hash pow rab-p rab-m r)))))))

(comment
  ; find common substrings in O(n)
  (let [some-data "abcdefghabcdefzz5"
        window-size 3
        windowed (windowed-with-outbyte window-size (.getBytes some-data))
        hash-seq (rolling-hash-seq window-size RAB-PRIME RAB-MOD windowed)
        char-seq (->> (.getBytes some-data)
                      (partition window-size 1 [0])
                      (map #(String. (byte-array %))))]
    (->> (interleave hash-seq char-seq)
         (partition-all 2)
         (group-by first))))
