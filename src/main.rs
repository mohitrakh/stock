use std::io;

use chrono::{DateTime, Utc};

#[derive(Debug)]
struct Order {
    id: u128,
    symbol: String,
    side: Side,
    price: u128,
    quantity: u128,
    remaining_quantity: u128,
    timestamp: DateTime<Utc>,
    status: Status,
}

impl Order {
    fn new(id: u128, symbol: String, side: Side, price: u128, quantity: u128) -> Self {
        Order {
            id,
            symbol,
            side,
            price,
            quantity,
            remaining_quantity: quantity,
            timestamp: Utc::now(),
            status: Status::NEW,
        }
    }
}
#[derive(Debug)]
enum Status {
    NEW,
    PARTIAL,
    FILLED,
    CANCELLED,
}
#[derive(Debug)]
enum Side {
    BUY,
    SELL,
}

fn main() {
    println!("WELCOME TO STOCK EXCHANGE!!!");
    let mut orders: Vec<Order> = Vec::new();

    let mut next_id: u128 = 1;

    loop {
        println!();
        println!("1. Create Order");
        println!("2. Print Orders");
        println!("3. Exit");

        let mut input = String::new();

        io::stdin().read_line(&mut input).expect("Failed to read");

        let choice: u32 = match input.trim().parse() {
            Ok(val) => val,
            Err(_) => return,
        };

        match choice {
            1 => {
                println!("Enter Symbol: ");
                let mut symbol = String::new();

                io::stdin().read_line(&mut symbol).expect("Failed to read");

                println!("Enter side (BUY/SELL): ");
                let mut side = String::new();

                io::stdin()
                    .read_line(&mut side)
                    .expect("Failed to read line");

                println!("Enter price: ");
                let mut price = String::new();

                io::stdin()
                    .read_line(&mut price)
                    .expect("Failed to read line");

                println!("Enter quantity: ");
                let mut quantity = String::new();

                io::stdin()
                    .read_line(&mut quantity)
                    .expect("Failed to read line");

                let price: u128 = price.trim().parse().expect("Invalid number");
                let quantity: u128 = quantity.trim().parse().expect("Invalid number");

                let side: Side = match side.trim().to_uppercase().as_str() {
                    "BUY" => Side::BUY,
                    "SELL" => Side::SELL,
                    _ => {
                        println!("Invalide side");
                        continue;
                    }
                };
                let order = Order::new(next_id, symbol, side, price, quantity);

                orders.push(order);

                continue;
            }
            2 => {
                println!("Orders: {:#?}", orders);
            }
            3 => {
                println!("Exiting....");
                return;
            }
            _ => {
                println!("Invalid Option!!");
            }
        }
    }
}
