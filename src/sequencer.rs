#[derive(Debug)]
pub struct Sequencer {
    next_seq: u64,
}

impl Sequencer {
    pub fn new(start_seq: u64) -> Self {
        Sequencer {
            next_seq: start_seq,
        }
    }

    pub fn next(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }
}
