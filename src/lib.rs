#![allow(dead_code)]
#![feature(test)]
extern crate test;

use std::cmp::Ordering;

fn parallel_k_way_merge<T: Copy + Ord + Send + Sync>(
    result: &mut [T],
    mut parts: Vec<&[T]>,
) -> Option<()> {
    parts.retain(|x| !x.is_empty());

    const CUTOFF: usize = 5000;
    let total_len = parts.iter().map(|x| x.len()).sum::<usize>();
    if total_len < CUTOFF || parts.len() == 1 {
        match parts.len() {
            0 => {}
            1 => result.copy_from_slice(parts[0]),
            2 => itertools::merge(parts[0].iter(), parts[1].iter())
                .zip(result)
                .for_each(|(src, dst)| *dst = *src),
            _ => itertools::kmerge(parts)
                .zip(result)
                .for_each(|(src, dst)| *dst = *src),
        }
    } else {
        let biggest = parts.iter().max_by_key(|x| x.len()).unwrap();
        let mid_pos = biggest.len() / 2;
        let pivot = biggest[mid_pos];
        let (left, right): (Vec<_>, Vec<_>) = parts
            .iter_mut()
            .map(|part| {
                let split_pos = part
                    .binary_search_by(|element| match element.cmp(&pivot) {
                        Ordering::Equal => Ordering::Greater,
                        ord => ord,
                    })
                    .unwrap_err();
                part.split_at(split_pos)
            })
            .unzip();
        let left_size = left.iter().map(|x| x.len()).sum::<usize>();
        let (left_result, right_result) = result.split_at_mut(left_size);
        rayon::join(
            || parallel_k_way_merge(left_result, left),
            || parallel_k_way_merge(right_result, right),
        );
    }
    Some(())
}

#[inline]
pub fn merge<T: Copy + Ord + Send + Sync>(result: &mut [T], parts: Vec<&[T]>) {
    parallel_k_way_merge(result, parts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[bench]
    fn bench_parallel_k_way_merge(b: &mut Bencher) {
        let one = (0..500000).into_iter().collect::<Vec<_>>();
        let all = (0..20).into_iter().map(|_| one.clone()).collect::<Vec<_>>();
        let parts = all.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let total_len = parts.iter().map(|x| x.len()).sum::<usize>();
        let mut result_buf = Vec::with_capacity(total_len);
        result_buf.resize_with(total_len, Default::default);

        b.iter(|| {
            let result_buf = black_box(result_buf.as_mut_slice());
            let parts = black_box(parts.clone());
            parallel_k_way_merge(result_buf, parts);
        });
    }

    #[bench]
    fn bench_parallel_memcpy(b: &mut Bencher) {
        let one = (0..500000).into_iter().collect::<Vec<_>>();
        let all = (0..20).into_iter().map(|_| one.clone()).collect::<Vec<_>>();
        let parts = all.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let total_len = parts.iter().map(|x| x.len()).sum::<usize>();
        let mut result_buf = Vec::with_capacity(total_len);
        result_buf.resize_with(total_len, Default::default);

        b.iter(|| {
            let mut result_buf = black_box(result_buf.as_mut_slice());
            let parts = black_box(parts.clone());

            rayon::scope(|s| {
                for part in parts {
                    let (current, res) = result_buf.split_at_mut(part.len());
                    result_buf = res;
                    s.spawn(|_| current.copy_from_slice(part));
                }
            });
        });
    }

    #[test]
    fn it_works() {
        let one = (0..10000).into_iter().collect::<Vec<_>>();
        let all = (0..20).into_iter().map(|_| one.clone()).collect::<Vec<_>>();
        let parts = all.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let total_len = parts.iter().map(|x| x.len()).sum::<usize>();
        let mut result_buf = Vec::with_capacity(total_len);
        result_buf.resize_with(total_len, Default::default);

        parallel_k_way_merge(result_buf.as_mut_slice(), parts);
        let mut result_correct = all.concat();
        result_correct.sort();
        assert_eq!(result_buf, result_correct);
    }
}
