mod types;
mod order_book;
mod matching_engine;
mod order_manager;
mod risk_manager;
mod wallet;
mod sequencer;

use types::Order;
use order_manager::OrderManager;

fn main() {
    let mut om = OrderManager::new();

    // Place a few orders
    let o1 = Order::new(
        "o1".into(),
        "u1".into(),
        "AAPL".into(),
        "SELL",
        100.0,
        10,
        None,
        1.0,
        0,
    )
    .unwrap();
    let o2 = Order::new(
        "o2".into(),
        "u2".into(),
        "AAPL".into(),
        "BUY",
        100.0,
        7,
        None,
        2.0,
        0,
    )
    .unwrap();

    om.add_order(o1).unwrap();
    println!("After o1: state = {:?}", om.get_state("o1"));

    om.add_order(o2).unwrap();
    println!("After o2: state = {:?}", om.get_state("o2"));

    // Cancel o1
    om.cancel_order("o1").unwrap();
    println!("State o1 after cancel: {:?}", om.get_state("o1"));
}
