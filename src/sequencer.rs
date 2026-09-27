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

    pub fn peek(&self) -> u64 {
        self.next_seq
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.next_seq
    }

    pub fn commit(&mut self, seq: u64) {
        assert_eq!(seq, self.next_seq, "prepared sequence must commit in order");
        self.next_seq += 1;
    }
}
