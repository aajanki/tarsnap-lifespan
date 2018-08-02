extern crate chrono;
use chrono::prelude::*;
use chrono::Duration;


pub trait SnapshotTimestamp {
    fn timestamp(&self) -> DateTime<Utc>;
}

#[derive(Clone, PartialEq, Debug)]
struct Snapshot {
    name: String,
    ts: DateTime<Utc>,
}

impl SnapshotTimestamp for Snapshot {
    fn timestamp(&self) -> DateTime<Utc> {
        return self.ts;
    }
}

fn main() {}

fn filter_by_interval<T: SnapshotTimestamp + Clone + PartialEq>(
    timestamps: &Vec<T>,
    interval: &Duration,
    now: &DateTime<Utc>,
) -> Vec<T> {
    timestamps.iter().map(|x| x.timestamp()).min().map_or(Vec::new(), |t| {
        filter_range(&timestamps, &interval, &t, &now)
    })
}

fn filter_range<T: SnapshotTimestamp + Clone + PartialEq>(
    timestamps: &Vec<T>,
    interval: &Duration,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Vec<T> {
    let mut selected = (0..)
        .map(|i| *end - *interval * i)
        .take_while(|t| t > start)
        .fold(Vec::new(), |mut acc, step| {
            let closest = timestamps.iter().min_by_key(|t| {
                (t.timestamp().timestamp() - step.timestamp()).abs()
            });

            if let Some(x) = closest {
                if !acc.contains(x) {
                    acc.push(x.clone());
                }
            }

            acc
        });

    selected.sort_unstable_by(|a, b| a.timestamp().cmp(&b.timestamp()));

    selected
}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Debug)]
    struct TestSnapshot {
        ts: DateTime<Utc>,
    }

    impl SnapshotTimestamp for TestSnapshot {
        fn timestamp(&self) -> DateTime<Utc> {
            return self.ts;
        }
    }

    #[test]
    fn filter_empty() {
        let snapshots: Vec<Snapshot> = vec![];
        let interval_day = Duration::days(1);
        let now = Utc.ymd(2018, 7, 14).and_hms(14, 0, 0);
        let filtered = filter_by_interval(&snapshots, &interval_day, &now);

        assert_eq!(filtered, []);
    }

    #[test]
    fn filter_one() {
        let snapshots = vec![TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) }];
        let interval_day = Duration::days(1);
        let now = Utc.ymd(2018, 7, 14).and_hms(14, 0, 0);
        let filtered = filter_by_interval(&snapshots, &interval_day, &now);

        assert_eq!(filtered, snapshots);
    }

    #[test]
    fn filter_even_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 17).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 17).and_hms(12, 0, 0) },
        ];
        let interval_day = Duration::days(1);
        let now = Utc.ymd(2018, 7, 17).and_hms(14, 0, 0);
        let expected = vec![
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 17).and_hms(12, 0, 0) },
        ];

        let filtered = filter_by_interval(&snapshots, &interval_day, &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn filter_uneven_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 3, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 3, 18).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 3, 27).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 0, 0) },
        ];
        let interval_month = Duration::days(30);
        let now = Utc.ymd(2018, 4, 5).and_hms(0, 0, 0);
        let expected = vec![
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 3, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 0, 0) },
        ];

        let filtered = filter_by_interval(&snapshots, &interval_month, &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn filter_hourly_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(2, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(3, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(3, 1, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 25, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(1, 10, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(2, 30, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(2, 40, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(3, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(3, 15, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(3, 20, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(6, 15, 0) },
        ];
        let interval_hour = Duration::hours(1);
        let now = Utc.ymd(2018, 4, 1).and_hms(10, 30, 0);
        let expected = vec![
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(2, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(3, 1, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(0, 25, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(1, 10, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(2, 30, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(3, 20, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(6, 15, 0) },
        ];

        let filtered = filter_by_interval(&snapshots, &interval_hour, &now);
        assert_eq!(filtered, expected);
    }
}
