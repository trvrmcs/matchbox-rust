use std::io;
use std::io::BufRead;
use std::str::FromStr;

use rust_decimal::prelude::*;
use std::cmp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideParseError(());

impl FromStr for Side {
    type Err = SideParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(SideParseError(())),
        }
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
    closed: BTreeSet<Uuid>,
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

#[derive(Debug)]
enum Command {
    Place(Place),
    Cancel(Uuid),
    Flush(),
}

#[derive(Debug)]
struct CommandAtTime {
    now: u64,
    command: Command,
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

    fn expiry(&self) -> u64 {
        match self.tif {
            TimeInForce::IOC => self.created,
            TimeInForce::GTC => self.created + MAX_LIFETIME,
            TimeInForce::GTD(lifetime) => self.created + lifetime,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct PriceTime(Decimal, u64);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct SidePriceTime(Side, Decimal, u64);




struct Engine {
    buy: BTreeMap<PriceTime, Order>,
    sell: BTreeMap<PriceTime, Order>,
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

fn merge(r1: MatchResult, closed: BTreeSet<Uuid>) -> MatchResult {
    MatchResult {
        fills: r1.fills,
        closed: r1.closed.union(&closed).map(|u| u.clone()).collect(),
    }
}

impl Engine {
    fn _match(&mut self, taker: &mut Order) -> MatchResult {
        let mut result = MatchResult {
            closed: BTreeSet::new(),
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

    fn resting(&mut self, side: Side) -> &mut BTreeMap<PriceTime, Order> {
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

    fn insert(&mut self, order: Order) {
        /*
            sort by price/time for SELL
            sort by (-price)/time for BUY
        */
        if let Some(_uuid) = self.uuid_to_side_price_time.insert(
            order.uuid,
            SidePriceTime(order.side, order.price, order.created),
        ) {
            panic!("Duplicate UUID: {}", order.uuid);
        }

        self.expiry_to_uuid.insert(order.expiry(), order.uuid);

        match order.side {
            Side::Buy => self
                .buy
                .insert(PriceTime(-order.price, order.created), order),
            Side::Sell => self
                .sell
                .insert(PriceTime(order.price, order.created), order),
        };
    }

    fn place(&mut self, command: Place, now: u64) -> MatchResult {
        let mut order: Order = Order::create(command, now);
        let result: MatchResult = self._match(&mut order);

        // Remove any closed orders from memory
        for uuid in &result.closed {
            self.remove(*uuid);
        }

        //add order to resting book if not immediately closed
        if !result.closed.contains(&order.uuid) {
            self.insert(order);
        }
        result
    }

    fn cancel(&mut self, uuid: Uuid) -> BTreeSet<Uuid> {
        if self.remove(uuid) {
            BTreeSet::from([uuid])
        } else {
            BTreeSet::new()
        }
    }

    fn remove(&mut self, uuid: Uuid) -> bool {
        /*
            Remove from uuid_to_side_price_time, get (side, price, time)
            Remove from self.buy/self.sell using (price,time)
            Remove from expiry_to_uuid using order.expiry
        */
        let result = self.uuid_to_side_price_time.remove(&uuid);

        if let Some(SidePriceTime(side, price, time)) = result {
            let r = match side {
                Side::Buy => self.buy.remove(&PriceTime(-price, time)),
                Side::Sell => self.sell.remove(&PriceTime(price, time)),
            };
            if let Some(order) = r {
                let expiry = order.expiry();
                let r2 = self.expiry_to_uuid.remove(&expiry);
                assert!(r2.is_some(), "ts missing in expiry_to_uuid");
                true
            } else {
                panic!("Data structure mismatch")
            }
        } else {
            false
        }
    }
    fn flush(&mut self, now: &u64) -> BTreeSet<Uuid> {
        let mut expired: BTreeSet<Uuid> = BTreeSet::new();

        for (expiry, uuid) in &self.expiry_to_uuid {
            if expiry <= now {
                expired.insert(*uuid);
            } else {
                break;
            }
        }

        for uuid in &expired {
            self.remove(*uuid);
        }
        expired
    }

    fn call(&mut self, command_at_time: CommandAtTime) -> MatchResult {
        /*
            I think we should always flush before a place or a cancel
        */
        let now = command_at_time.now;
        let command = command_at_time.command;

        if now <= self.last_tick {
            panic!(
                "current_tick:{} must be greater than last_tick:{}",
                now, self.last_tick
            );
        }
        self.last_tick = now;
        let result = match command {
            Command::Place(place) => {
                let flushed = self.flush(&now);
                let result = self.place(place, now);
                merge(result, flushed)
            }
            Command::Cancel(uuid) => {
                let flushed = self.flush(&now);
                let result = MatchResult {
                    fills: Vec::new(),
                    closed: self.cancel(uuid),
                };
                merge(result, flushed)
            }
            Command::Flush() => MatchResult {
                fills: Vec::new(),
                closed: self.flush(&now),
            },
        };
        result
    }
}

/*
Input/Output
*/

fn time_in_force(slice: &[String]) -> TimeInForce {
    let name: &str = &slice[0];
    match name {
        "IOC" => TimeInForce::IOC,
        "GTC" => TimeInForce::GTC,
        "GTD" => {
            if let Some(lifetime_s) = slice.get(1) {
                /*lifetime probably has to be >0*/
                let lifetime = u64::from_str(lifetime_s).unwrap();
                if lifetime < 1 {
                    panic!("lifetime must be greater than zero")
                }
                TimeInForce::GTD(lifetime)
            } else {
                panic!("Can't parse TIF: {}", name)
            }
        }
        _ => panic!("Can't parse TIF: {}", name),
    }
}

fn limit_order_command(slice: &[String]) -> Command {
    Command::Place(Place::LimitOrder {
        uuid: Uuid::from_str(&slice[0]).unwrap(),
        side: Side::from_str(&slice[1]).unwrap(),
        amount: Decimal::from_str(&slice[2]).unwrap(),
        price: Decimal::from_str(&slice[3]).unwrap(),
        tif: time_in_force(&slice[4..]),
    })
}
fn market_order_command(slice: &[String]) -> Command {
    Command::Place(Place::MarketOrder {
        uuid: Uuid::from_str(&slice[0]).unwrap(),
        side: Side::from_str(&slice[1]).unwrap(),
        amount: Decimal::from_str(&slice[2]).unwrap(),
    })
}
fn cancel_command(slice: &[String]) -> Command {
    if let Some(uuid_s) = slice.get(0) {
        Command::Cancel(Uuid::from_str(uuid_s).unwrap())
    } else {
        panic!("Can't parse cancel command")
    }
}
fn parse_line(line: String) -> CommandAtTime {
    /*Might be faster to avoid collect*/
    let v: Vec<String> = line.split(",").map(|s| s.to_string()).collect();

    let now: u64 = (&v[0]).parse().unwrap();
    let name: &str = &v[1];

    let command = match name {
        "flush" => Command::Flush(),
        "limit" => limit_order_command(&v[2..]),
        "market" => market_order_command(&v[2..]),
        "cancel" => cancel_command(&v[2..]),
        _ => panic!("Can't parse: {}", name),
    };

    CommandAtTime {
        now: now,
        command: command,
    }
}

fn print_result(result: &MatchResult, now: u64) {
    for fill in &result.fills {
        println!(
            "< {},fill,{},{},{},{}",
            now, fill.maker_uuid, fill.taker_uuid, fill.base_amount, fill.price
        );
    }
    /*Would be good to sort this to get identical output to Python*/
    for uuid in &result.closed {
        println!("< {},closed,{}", now, uuid);
    }
}

fn main() {
    let mut engine = Engine::new();
    let stdin = io::stdin();

    for line in stdin.lock().lines().map(|line| line.unwrap()) {
        println!("> {}", line);
        let command_at_time = parse_line(line);
        let now = command_at_time.now;
        let result = engine.call(command_at_time);
        print_result(&result, now);
    }
}
