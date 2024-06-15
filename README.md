# rayon k way merge

A Fast Parallel K-way Merging Implementation

```plain
   Compiling rayon-k-way-merge v0.1.0 (/home/jyi/dev/rayon-k-way-merge)
    Finished `bench` profile [optimized] target(s) in 0.49s
     Running unittests src/lib.rs (/home/jyi/.cargo/target/release/deps/rayon_k_way_merge-c1327e3231de6784)

running 3 tests
test tests::it_works ... ignored
test tests::bench_parallel_k_way_merge ... bench:  13,352,235.40 ns/iter (+/- 1,662,932.45)
test tests::bench_parallel_memcpy      ... bench:   9,050,300.60 ns/iter (+/- 667,321.61)

test result: ok. 0 passed; 0 failed; 1 ignored; 2 measured; 0 filtered out; finished in 12.59s
```
