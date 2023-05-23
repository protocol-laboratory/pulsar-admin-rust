use rand::Rng;

const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

pub fn rand_str(n: usize) -> String {
    let mut rng = rand::thread_rng();
    let mut b = vec![0u8; n];
    for i in 0..n {
        b[i] = LETTERS[rng.gen_range(0..LETTERS.len())];
    }
    String::from_utf8(b).unwrap()
}
