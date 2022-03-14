use std::io;
use std::io::BufRead;

use rust_decimal::prelude::*;
use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use uuid::Uuid;

// time is in nanoseconds
const SECOND: u64 = 1000 * 1000 * 1000;
const DAY: u64 = SECOND * 60 * 60 * 24;
const MAX_LIFETIME: u64 = 90 * DAY;

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug)]
enum Side {
    Buy,
    Sell,
}

fn other_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

#[derive(Copy, Clone, Debug)]
enum TimeInForce {
    GTC,
    IOC,
    GTD(u64), // lifetime in nanoseconds
}
/*

    Not sure yet how to specify (im)mutability in
    nested structs in rust.

    I'd like to make Order mostly immutable, apart from `remaining_amount`,
    but it has to live inside a BTreeMap which is obviously mutable.
*/
struct Order {
    uuid: Uuid,
    side: Side,
    created: u64,
    amount: Decimal,
    price: Decimal,
    tif: TimeInForce,
    //This is the only field that needs to be mutable; maybe
    // we should use Cell<Decimal> ??
    remaining_amount: Decimal,
}
#[derive(Debug)]
struct Fill {
    base_amount: Decimal,
    price: Decimal,
    maker_uuid: Uuid,
    taker_uuid: Uuid,
}

impl Fill {
    fn quote_amount(&self) -> Decimal {
        return self.base_amount * self.price;
    }
}

#[derive(Debug)]
struct MatchResult {
    fills: Vec<Fill>,
    closed: HashSet<Uuid>,
}

#[derive(Debug)]
enum Place {
    MarketOrder {
        uuid: Uuid,
        side: Side,
        amount: Decimal,
    },
    LimitOrder {
        uuid: Uuid,
        side: Side,
        amount: Decimal,
        tif: TimeInForce,
        price: Decimal,
    },
}


/**/
#[derive(Debug)]
enum Command {
    Place(Place),
    Cancel(Uuid),
    Flush,
}

impl Order {
    fn create(place: Place, now: u64) -> Order {
        match place {
            Place::MarketOrder { uuid, side, amount } => Order {
                uuid: uuid,
                created: now,
                side: side,
                amount: amount,
                tif: TimeInForce::IOC,
                price: match side {
                    Side::Buy => Decimal::MAX,
                    Side::Sell => Decimal::ZERO,
                },
                remaining_amount: amount,
            },
            Place::LimitOrder {
                uuid,
                side,
                amount,
                tif,
                price,
            } => Order {
                uuid: uuid,
                created: now,
                side: side,
                amount: amount,
                tif: tif,
                price: price,
                remaining_amount: amount,
            },
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct PriceTime(Decimal, u64);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct SidePriceTime(Side, Decimal, u64);

type Orders = BTreeMap<PriceTime, Order>;

struct Engine {
    /*
        I want K to be price/ts/uuid
        and V to be an Order

        Not sure yet how we do the
        reverse ordering trick from sortedcontainers,
        we may have to walk the buy book backwards.

        In python we maintain multiple indices, so
        we can find orders by uuid
        and sort them by (price/time) and by (time)

        Getting the data structures right is the key design
        challenge
    */
    buy: Orders,
    sell: Orders,
    last_tick: u64,
    uuid_to_side_price_time: HashMap<Uuid, SidePriceTime>,
    expiry_to_uuid: BTreeMap<u64, Uuid>,
}

fn crossed(taker: &Order, maker: &Order) -> bool {
    if taker.remaining_amount.is_zero() {
        return false;
    };
    match taker.side {
        Side::Buy => taker.price >= maker.price,
        Side::Sell => taker.price <= maker.price,
    }
}

fn expiry(order: &Order, now: u64) -> u64 {
    match order.tif {
        TimeInForce::IOC => now,
        TimeInForce::GTC => now + MAX_LIFETIME,
        TimeInForce::GTD(lifetime) => now + lifetime,
    }
}

impl Engine {
    fn _match(&mut self, taker: &mut Order) -> MatchResult {
        let mut result = MatchResult {
            closed: HashSet::new(),
            fills: Vec::new(),
        };

        let resting = &mut self.resting(other_side(taker.side));
        for (_, maker) in resting.iter_mut() {
            if !crossed(&taker, &maker) {
                break;
            }

            if taker.remaining_amount <= maker.remaining_amount {
                result.closed.insert(taker.uuid);
            }
            if taker.remaining_amount >= maker.remaining_amount {
                result.closed.insert(maker.uuid);
            }

            let fill = Fill {
                base_amount: cmp::min(taker.remaining_amount, maker.remaining_amount),
                price: maker.price,
                maker_uuid: maker.uuid,
                taker_uuid: taker.uuid,
            };

            taker.remaining_amount -= fill.base_amount;
            maker.remaining_amount -= fill.base_amount;

            result.fills.push(fill); //now 'fill' belongs to 'result'
        }
        if let TimeInForce::IOC = taker.tif {
            result.closed.insert(taker.uuid);
        }

        result
    }

    fn resting(&mut self, side: Side) -> &mut Orders {
        match side {
            Side::Buy => &mut self.buy,
            Side::Sell => &mut self.sell,
        }
    }
    fn new() -> Engine {
        Engine {
            buy: BTreeMap::new(),
            sell: BTreeMap::new(),
            last_tick: 0,
            uuid_to_side_price_time: HashMap::new(),
            expiry_to_uuid: BTreeMap::new(),
        }
    }

    fn insert(&mut self, order: Order, now: u64) {
        
        if let Some(uuid)=self.uuid_to_side_price_time.insert(order.uuid, SidePriceTime(order.side, order.price, now)){
            panic!("Duplicate UUID: {}", order.uuid);
        }
        
        self.expiry_to_uuid.insert(expiry(&order, now), order.uuid);

        match order.side {
            Side::Buy => self.buy.insert(PriceTime(-order.price, now), order),
            Side::Sell => self.sell.insert(PriceTime(order.price, now), order),
        };
    }

    fn place(&mut self, command: Place, now: u64) -> MatchResult {
        assert!(now > self.last_tick, "now<= last_tick");

        let mut order: Order = Order::create(command, now);
        let result: MatchResult = self._match(&mut order);
        if !result.closed.contains(&order.uuid) {
            self.insert(order, now);
        }
        result
    }

    fn cancel(&mut self, uuid: Uuid) -> MatchResult {
        /*

            Remove from uuid_to_side_price_time, get (side, price, time)
            Remove from self.buy/self.sell using (price,time)
            Remove from ts_to_uuid using time

        */
        if let Some(_u) = &self.pop(uuid) {
            MatchResult {
                closed: HashSet::from([uuid]),
                fills: Vec::new(),
            }
        } else {
            MatchResult {
                closed: HashSet::new(),
                fills: Vec::new(),
            }
        }
    }

    fn pop(&mut self, uuid: Uuid) -> Option<Uuid> {
        let result = self.uuid_to_side_price_time.remove(&uuid);

        if let Some(SidePriceTime(side, price, time)) = result {
            let r = match side {
                Side::Buy => self.buy.remove(&PriceTime(-price, time)),
                Side::Sell => self.sell.remove(&PriceTime(price, time)),
            };
            assert!(r.is_some(), "Data structure mismatch");
            //let r2=self.ts_to_uuid.remove(&time);
            //assert!(r2.is_some(),"ts missing in ts_to_uuid");
            Some(uuid)
        } else {
            None
        }
    }
    fn flush(&mut self, now: &u64) -> MatchResult {
        let mut to_flush: HashSet<Uuid> = HashSet::new();

        for (expiry, uuid) in &self.expiry_to_uuid {
            if expiry <= now {
                to_flush.insert(*uuid);
            } else {
                break;
            }
        }

        for uuid in &to_flush {
            self.pop(*uuid);
        }

        MatchResult {
            closed: to_flush,
            fills: Vec::new(),
        }
    }

    fn call(&mut self, cmd: Command, now: u64) -> MatchResult {
        match cmd {
            Command::Place(place) => self.place(place, now),
            Command::Cancel(uuid) => self.cancel(uuid),
            Command::Flush => self.flush(&now),
        }
    }
}

fn apply(engine: &mut Engine, command: Command, now: u64) {
    println!("Command: {:?}\n", command);
    let result = engine.call(command, now);

    println!("Result: {:?}\n", result);
}

fn str_to_side(s: &str) -> Side {
    match s {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => panic!("Can't parse side: {}", s),
    }
}

fn limit_order_command(slice: &[String]) -> Command {
    Command::Place(Place::LimitOrder {
        uuid: Uuid::parse_str(&slice[0]).unwrap(),
        side: str_to_side(&slice[1]),
        amount: Decimal::from_str(&slice[2]).unwrap(),
        price: Decimal::from_str(&slice[3]).unwrap(),
        tif: TimeInForce::GTC,
    })
}
fn market_order_command(slice: &[String]) -> Command {
    Command::Place(Place::MarketOrder {
        uuid: Uuid::parse_str(&slice[0]).unwrap(),
        side: str_to_side(&slice[1]),
        amount: Decimal::from_str(&slice[2]).unwrap(),
    })
}

fn parseLine(line: String) -> Command {
    println!("{}",line);
    let v: Vec<String> = line.split(",").map(|s| s.to_string()).collect();

    let name: &str = &v[0];
    match name {
        "flush" => Command::Flush,
        "limit" => limit_order_command(&v[1..]),
        "market" => market_order_command(&v[1..]),
        _ => panic!("Can't parse: {}", name),
    }
}

fn printResult(result:&MatchResult){
    for fill in &result.fills{
        println!("fill,{},{},{},{}",fill.maker_uuid, fill.taker_uuid, fill.base_amount, fill.price);    
    }
    for uuid in &result.closed{
        println!("closed,{}",uuid);
    }
    
    
}

fn main() {
    let mut engine = Engine::new();
    let stdin = io::stdin();
    let mut now = 0;
    for command in stdin
        .lock()
        .lines()
        .map(|line| line.unwrap())
        .map(parseLine)
    {
        now += 1;
        // apply(&mut engine, command, now);
        let result = engine.call(command, now);
        printResult(&result)
    }
}

fn main_was() {
    let mut engine = Engine::new();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let mut now = 1;
    apply(
        &mut engine,
        Command::Place(Place::LimitOrder {
            uuid: u1,
            side: Side::Buy,
            amount: Decimal::new(2, 0),
            price: Decimal::new(100, 0),
            tif: TimeInForce::GTC,
        }),
        now,
    );

    now += 1;
    apply(
        &mut engine,
        Command::Place(Place::MarketOrder {
            uuid: u2,
            side: Side::Sell,
            amount: Decimal::new(1, 0),
        }),
        now,
    );

    now += 1;
    apply(&mut engine, Command::Cancel(u1), now);
    now += 1;

    apply(
        &mut engine,
        Command::Place(Place::LimitOrder {
            uuid: u1,
            side: Side::Buy,
            amount: Decimal::new(2, 0),
            price: Decimal::new(100, 0),
            tif: TimeInForce::GTD(now + 10),
        }),
        now,
    );

    now += 5;
    apply(&mut engine, Command::Flush, now);
    now += 20;
    apply(&mut engine, Command::Flush, now);
}
