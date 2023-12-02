(ns clj-rabin.core)

(defn interleave-outbyte
  "Take a partitioned seq of bytes and weave the byte leaving
  the front of the sliding window at each step, e.g.
  [1 2 3 4 5] with window size 2:
  [[nil [1 2]] [1 [2 3]] ...]"
  [out-byte partitioned-bytes]
  (lazy-seq
    (when-let [[p & ps] partitioned-bytes]
      (cons [out-byte p] (interleave-outbyte (first p) ps)))))

(defn outbyte-windowed
  "Given a byte array, emit a lazy seq of each sliding window as
  [prev-byte [bytes...]]..."
  [^Integer window-size ^bytes bs]
  (->> bs
       (partition window-size 1 [0])
       (interleave-outbyte 0)))

(defn pow-mod
  "b^e mod q each step"
  [q b e]
  (->> (repeat b)
       (take e)
       (reduce #(mod (* %1 %2) q) 1)))

(defn hash-window
  "Hash bytes using base p and modulus q"
  [{:keys [p q]} bs]
  (reduce (fn [prev b]
           (mod (+ (* prev p)
                   (int b)) q))
    0 bs))

(defn roll-hash
  "Given a previous hash value, apply the current byte and remove the out-byte"
  [{:keys [pow p q]} prev-hash in-byte out-byte]
  ; ((prev-hash * p) - (pow * b[0]) + b[n]) % q
  (-> (* p prev-hash)
      (- (* pow out-byte))
      (+ in-byte)
      (mod q)))

(defn rolling-hash-seq
  "Given a context of Rabin constants and some bytes, emit a seq of rolling
  rabin hashes"
  ([{:keys [p q window-size] :as ctx} ^bytes bs]
   (let [[[_ window] & windows] (outbyte-windowed window-size bs)]
     (rolling-hash-seq
       (assoc ctx :pow (pow-mod q p window-size))
       (hash-window ctx window)
       windows)))
  ([ctx prev-hash windows]
   (lazy-seq
     (when (and ctx windows)
       (let [[[out-byte window] & r] windows
             next-hash (roll-hash ctx prev-hash (last window) out-byte)]
         (cons next-hash (rolling-hash-seq ctx next-hash r)))))))

(comment
  ; find common windows in O(n)
  (let [some-data "abcdefghabcdefzabcdz5"
        rabin-ctx {:window-size 2 :p 256 :q 153191}
        hash-seq (rolling-hash-seq rabin-ctx (.getBytes some-data))
        char-seq (->> (.getBytes some-data)
                      (partition (:window-size rabin-ctx) 1 [0])
                      (map #(String. (byte-array %))))]
    (->> (interleave hash-seq char-seq)
         (partition-all 2)
         (group-by first))))
