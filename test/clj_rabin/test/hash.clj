(ns clj-rabin.test.hash
  (:require [clojure.test :refer :all]
            [clj-rabin.hash :as hash]
            [clojure.pprint :as pprint]))

(deftest test-hash-seq
  ; https://leetcode.com/problems/repeated-dna-sequences
  (testing "it can identify repeated sequences"
    (let [test-cases [["AAAAACCCCCAAAAACCCCCCAAAAAGGGTTT"
                       #{"AAAAACCCCC","CCCCCAAAAA"}]
                      ["AAAAAAAAAAAAA"
                       #{"AAAAAAAAAA"}]]
          window-size 10]
      (doseq [[dna assert-chunks] test-cases
              :let [hashes (hash/byte-array->hash-seq {:window-size window-size}
                                                      (.getBytes dna))
                    chunks (->> (map (fn [[i h]] [(inc i) h]) hashes)
                                (group-by last)
                                (filter #(> (count (last %)) 1))
                                (mapcat last)
                                (map first)
                                (map #(subs dna (- % window-size) %))
                                (into #{}))]]
        (is (= chunks assert-chunks))))))
