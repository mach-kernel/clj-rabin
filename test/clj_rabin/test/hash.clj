(ns clj-rabin.test.hash
  (:require [clojure.test :refer :all]
            [clj-rabin.hash :as hash]
            [clojure.pprint :as pprint]))

(defn hashes->groups
  [s window-size hashes]
  (->> (map (fn [[i h]] [(inc i) h]) hashes)
       (group-by last)
       (filter #(> (count (last %)) 1))
       (mapcat last)
       (map first)
       (map #(subs s (- % window-size) %))
       (into #{})))

(deftest test-hash-seq
  ; https://leetcode.com/problems/repeated-dna-sequences
  (testing "it can identify repeated sequences"
    (let [test-cases [["AAAAACCCCCAAAAACCCCCCAAAAAGGGTTT"
                       #{"AAAAACCCCC","CCCCCAAAAA"}]
                      ["AAAAAAAAAAAAA"
                       #{"AAAAAAAAAA"}]]
          window-size 10]
      (doseq [[dna assert-chunks] test-cases
              :let [ctx (hash/->hash-context {:window-size window-size})
                    hashes (hash/byte-array->hash-seq ctx (.getBytes dna))
                    chunks (hashes->groups dna window-size hashes)]]
        (is (= chunks assert-chunks)))))
  (testing "window size 1"
    (let [string "AAAAACCCCCAAAAACCCCCCAAAAAGGGTTT"
          {:keys [window-size] :as ctx} (hash/->hash-context {:window-size 1
                                                              :prime Integer/MAX_VALUE})
          hashes (hash/byte-array->hash-seq ctx (.getBytes string))
          chunks (hashes->groups string window-size hashes)]
      (is (= #{"A" "T" "C" "G"} chunks))))
  (testing "overflow"
    (let [string (byte-array (repeat 100 0xFF))]
      (hash/byte-array->hash-seq string))))