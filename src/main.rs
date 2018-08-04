use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::process::Command;

extern crate regex;
use regex::Regex;
extern crate chrono;
use chrono::prelude::*;
use chrono::{Duration, NaiveDateTime};
#[macro_use]
extern crate indoc;

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

struct Generation {
    interval: Duration,
    count: usize,
}

fn main() {
    let args: Vec<_> = env::args().collect();

    if args.len() <= 1 {
        eprintln!("Usage:");
        eprintln!("{} <number><H|D|M|Y> <...>", args[0]);
        std::process::exit(1);
    }

    let now = Utc::now();
    let res = parse_generations(args.iter().skip(1).cloned().collect()).and_then(|generations| {
        list_archives()
            .and_then(parse_archives)
            .map(|snapshots| {
                select_snapshots_to_delete(&generations, &now, snapshots)
            })
            .and_then(delete_snapshots)
    });

    if res.is_err() {
        eprintln!("ERROR: {}", res.unwrap_err());
        std::process::exit(1);
    }
}

fn parse_generations(generation_args: Vec<String>) -> Result<Vec<Generation>, String> {
    fn generation_count_from_arg(arg: &String, hours: i64) -> Result<Generation, String> {
        Ok(Generation {
            interval: Duration::hours(hours),
            count: arg[..arg.len() - 1].parse::<usize>().unwrap(),
        })
    }

    let hour_re = Regex::new(r"^(\d+)H$").unwrap();
    let day_re = Regex::new(r"^(\d+)D$").unwrap();
    let month_re = Regex::new(r"^(\d+)M$").unwrap();
    let year_re = Regex::new(r"^(\d+)Y$").unwrap();

    generation_args
        .iter()
        .map(|arg| if hour_re.is_match(arg) {
            generation_count_from_arg(arg, 1)
        } else if day_re.is_match(arg) {
            generation_count_from_arg(arg, 24)
        } else if month_re.is_match(arg) {
            generation_count_from_arg(arg, 30 * 24)
        } else if year_re.is_match(arg) {
            generation_count_from_arg(arg, 365 * 24)
        } else {
            let mut msg = "Failed to parse argument".to_string();
            msg.push_str(arg);
            Err(msg)
        })
        .collect()
}

fn list_archives() -> Result<String, String> {
    Command::new("/usr/bin/tarsnap")
        .arg("--list-archives")
        .arg("-v")
        .env("TZ", "0")
        .output()
        .map_err(|err| err.to_string())
        .and_then(|output| if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        })
}

// Parse the snapshot names and creation times from the "tarsnap
// --list-archives -v" output
fn parse_archives(archives: String) -> Result<Vec<Snapshot>, String> {
    archives
        .split_terminator('\n')
        .map(|row| parse_archive_row(row))
        .collect()
}

// Parse one line of --list-archives -v output. For example:
// archive-2018-07-16_11-01-03       2018-07-16 11:01:03
fn parse_archive_row(row: &str) -> Result<Snapshot, String> {
    let parts: Vec<&str> = row.splitn(2, '\t').collect();
    if parts.len() == 2 {
        parse_local_datetime_from_str(parts[1].trim()).map(|t| {
            Snapshot {
                name: parts[0].to_string(),
                ts: t,
            }
        })
    } else {
        let mut msg = "Failed to parse timestamp: ".to_string();
        msg.push_str(row);
        Err(msg)
    }
}

// Parse timestamp from string such as "2018-07-14 11:15:32"
fn parse_local_datetime_from_str(s: &str) -> Result<DateTime<Utc>, String> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .map(|t| DateTime::<Utc>::from_utc(t, Utc))
        .map_err(|err| err.description().to_string())
}

fn delete_snapshots(snapshot_names: Vec<String>) -> Result<(), String> {
    let mut sorted_names = snapshot_names.clone();
    sorted_names.sort_unstable();
    println!(
        "snapshots selected for deletion: {}",
        sorted_names.join(", ")
    );

    if sorted_names.is_empty() {
        // nothing to do
        Ok(())
    } else {
        let snapshot_name_args = sorted_names.iter().flat_map(|name| vec!["-f", name]);
        Command::new("/usr/bin/tarsnap")
            .arg("-d")
            .args(snapshot_name_args)
            .output()
            .map_err(|err| err.to_string())
            .and_then(|output| if output.status.success() {
                Ok(())
            } else {
                Err(String::from_utf8_lossy(&output.stderr).to_string())
            })
    }
}

fn select_snapshots_to_delete(
    generations: &Vec<Generation>,
    now: &DateTime<Utc>,
    snapshots: Vec<Snapshot>,
) -> Vec<String> {
    let all_names: HashSet<String> = snapshots.iter().map(|x| x.name.clone()).collect();
    let keep_names = keep_generations(&snapshots, generations, now);
    all_names.difference(&keep_names).cloned().collect()
}

fn keep_generations(
    snapshots: &Vec<Snapshot>,
    generations: &Vec<Generation>,
    now: &DateTime<Utc>,
) -> HashSet<String> {
    let mut selected: HashSet<String> = generations
        .iter()
        .flat_map(|gen| filter_by_generation(snapshots, gen, now))
        .map(|x| x.name)
        .collect();

    // Always keep the latest snapshot
    let maybe_latest = snapshots.iter().max_by_key(|x| x.timestamp()).map(|x| {
        x.name.clone()
    });
    if let Some(latest) = maybe_latest {
        selected.insert(latest);
    }

    selected
}

fn filter_by_generation<T: SnapshotTimestamp + Clone + PartialEq>(
    timestamps: &Vec<T>,
    generation: &Generation,
    now: &DateTime<Utc>,
) -> Vec<T> {
    let mut selected = (1..(generation.count + 1))
        .map(|i| *now - generation.interval * (i as i32))
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

    fn test_generations() -> Vec<Generation> {
        vec![
            Generation {
                interval: Duration::days(1),
                count: 6,
            },
            Generation {
                interval: Duration::days(30),
                count: 4,
            },
            Generation {
                interval: Duration::days(365),
                count: 1,
            },
        ]
    }

    fn hour_generation(n: usize) -> Generation {
        Generation {
            interval: Duration::hours(1),
            count: n,
        }
    }

    fn day_generation(n: usize) -> Generation {
        Generation {
            interval: Duration::days(1),
            count: n,
        }
    }

    fn month_generation(n: usize) -> Generation {
        Generation {
            interval: Duration::days(30),
            count: n,
        }
    }

    fn utc_midnight(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.ymd(year, month, day).and_hms(0, 0, 0)
    }

    #[test]
    fn filter_empty() {
        let snapshots: Vec<Snapshot> = vec![];
        let now = Utc.ymd(2018, 7, 14).and_hms(14, 0, 0);
        let filtered = filter_by_generation(&snapshots, &day_generation(6), &now);

        assert_eq!(filtered, []);
    }

    #[test]
    fn filter_one() {
        let snapshots = vec![TestSnapshot { ts: utc_midnight(2018, 7, 10) }];
        let now = Utc.ymd(2018, 7, 14).and_hms(14, 0, 0);
        let filtered = filter_by_generation(&snapshots, &day_generation(6), &now);

        assert_eq!(filtered, snapshots);
    }

    #[test]
    fn filter_even_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: Utc.ymd(2018, 7, 13).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 13).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 17).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 17).and_hms(12, 0, 0) },
        ];
        let now = Utc.ymd(2018, 7, 17).and_hms(14, 0, 0);
        let expected = vec![
            TestSnapshot { ts: Utc.ymd(2018, 7, 14).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 15).and_hms(12, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 7, 16).and_hms(12, 0, 0) },
        ];

        let filtered = filter_by_generation(&snapshots, &day_generation(3), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn filter_uneven_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 18) },
            TestSnapshot { ts: utc_midnight(2018, 3, 27) },
            TestSnapshot { ts: utc_midnight(2018, 4, 1) },
        ];
        let now = utc_midnight(2018, 4, 5);
        let expected = vec![
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
        ];

        let filtered = filter_by_generation(&snapshots, &month_generation(4), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn filter_hourly_intervals() {
        let snapshots = vec![
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 1, 1).and_hms(2, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(13, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(0, 25, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(1, 10, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(2, 30, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(2, 40, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(3, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(3, 15, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(3, 20, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(6, 15, 0) },
        ];
        let now = Utc.ymd(2018, 4, 2).and_hms(10, 30, 0);
        let expected = vec![
            TestSnapshot { ts: Utc.ymd(2018, 4, 1).and_hms(13, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(0, 0, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(0, 25, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(1, 10, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(2, 30, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(3, 20, 0) },
            TestSnapshot { ts: Utc.ymd(2018, 4, 2).and_hms(6, 15, 0) },
        ];

        let filtered = filter_by_generation(&snapshots, &hour_generation(24), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn generation_large_count() {
        let snapshots = vec![
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
            TestSnapshot { ts: utc_midnight(2018, 4, 1) },
        ];
        let generation = Generation {
            interval: Duration::days(30),
            count: 99,
        };
        let now = utc_midnight(2018, 4, 5);
        let expected = vec![
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
        ];

        let filtered = filter_by_generation(&snapshots, &generation, &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn generation_uneven() {
        let snapshots = vec![
            TestSnapshot { ts: utc_midnight(2017, 10, 1) },
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 18) },
            TestSnapshot { ts: utc_midnight(2018, 3, 27) },
            TestSnapshot { ts: utc_midnight(2018, 4, 1) },
        ];
        let generation = Generation {
            interval: Duration::days(30),
            count: 3,
        };
        let now = utc_midnight(2018, 4, 5);
        let expected = vec![
            TestSnapshot { ts: utc_midnight(2018, 1, 1) },
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
        ];

        let filtered = filter_by_generation(&snapshots, &generation, &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn zero_count_generation() {
        let snapshots = vec![
            TestSnapshot { ts: utc_midnight(2018, 3, 1) },
            TestSnapshot { ts: utc_midnight(2018, 4, 1) },
            TestSnapshot { ts: utc_midnight(2018, 5, 1) },
            TestSnapshot { ts: utc_midnight(2018, 6, 1) },
        ];
        let generation = Generation {
            interval: Duration::days(30),
            count: 0,
        };
        let now = Utc.ymd(2018, 8, 1).and_hms(12, 0, 0);

        let filtered = filter_by_generation(&snapshots, &generation, &now);
        assert_eq!(filtered, Vec::new());
    }

    #[test]
    fn generations_empty() {
        let snapshots = vec![];
        let now = Utc.ymd(2018, 8, 1).and_hms(12, 0, 0);

        let filtered = keep_generations(&snapshots, &test_generations(), &now);
        assert_eq!(filtered, HashSet::new());
    }

    #[test]
    fn generations_1() {
        let snapshots = vec![
            Snapshot {
                name: "two_years_ago".to_string(),
                ts: utc_midnight(2016, 6, 1),
            },
            Snapshot {
                name: "last_year".to_string(),
                ts: utc_midnight(2017, 6, 1),
            },
            Snapshot {
                name: "jan_1".to_string(),
                ts: utc_midnight(2018, 1, 1),
            },
            Snapshot {
                name: "feb_1".to_string(),
                ts: utc_midnight(2018, 2, 1),
            },
            Snapshot {
                name: "feb_27".to_string(),
                ts: utc_midnight(2018, 2, 27),
            },
            Snapshot {
                name: "feb_28".to_string(),
                ts: utc_midnight(2018, 2, 28),
            },
            Snapshot {
                name: "mar_1".to_string(),
                ts: utc_midnight(2018, 3, 1),
            },
            Snapshot {
                name: "apr_1".to_string(),
                ts: utc_midnight(2018, 4, 1),
            },
            Snapshot {
                name: "may_1".to_string(),
                ts: utc_midnight(2018, 5, 1),
            },
            Snapshot {
                name: "jun_1".to_string(),
                ts: utc_midnight(2018, 6, 1),
            },
            Snapshot {
                name: "jun_2".to_string(),
                ts: utc_midnight(2018, 6, 2),
            },
            Snapshot {
                name: "jun_3".to_string(),
                ts: utc_midnight(2018, 6, 3),
            },
            Snapshot {
                name: "jun_4".to_string(),
                ts: utc_midnight(2018, 6, 4),
            },
            Snapshot {
                name: "jun_5".to_string(),
                ts: utc_midnight(2018, 6, 5),
            },
            Snapshot {
                name: "jun_6".to_string(),
                ts: utc_midnight(2018, 6, 6),
            },
        ];
        let now = Utc.ymd(2018, 6, 6).and_hms(16, 0, 0);

        let expected: HashSet<String> = vec![
            "last_year".to_string(),
            "feb_1".to_string(),
            "mar_1".to_string(),
            "apr_1".to_string(),
            "may_1".to_string(),
            "jun_1".to_string(),
            "jun_2".to_string(),
            "jun_3".to_string(),
            "jun_4".to_string(),
            "jun_5".to_string(),
            "jun_6".to_string(),
        ].iter()
            .cloned()
            .collect();

        let filtered = keep_generations(&snapshots, &test_generations(), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn generations_2() {
        let snapshots = vec![
            Snapshot {
                name: "two_years_ago".to_string(),
                ts: utc_midnight(2016, 6, 1),
            },
            Snapshot {
                name: "last_year".to_string(),
                ts: utc_midnight(2017, 6, 1),
            },
            Snapshot {
                name: "jan_1".to_string(),
                ts: utc_midnight(2018, 1, 1),
            },
            Snapshot {
                name: "feb_1".to_string(),
                ts: utc_midnight(2018, 2, 1),
            },
            Snapshot {
                name: "mar_1".to_string(),
                ts: utc_midnight(2018, 3, 1),
            },
            Snapshot {
                name: "apr_1".to_string(),
                ts: utc_midnight(2018, 4, 1),
            },
            Snapshot {
                name: "may_1".to_string(),
                ts: utc_midnight(2018, 5, 1),
            },
            Snapshot {
                name: "jun_6".to_string(),
                ts: utc_midnight(2018, 6, 6),
            },
            Snapshot {
                name: "jun_7".to_string(),
                ts: utc_midnight(2018, 6, 7),
            },
            Snapshot {
                name: "jun_8".to_string(),
                ts: utc_midnight(2018, 6, 8),
            },
            Snapshot {
                name: "jun_9".to_string(),
                ts: utc_midnight(2018, 6, 9),
            },
            Snapshot {
                name: "jun_10".to_string(),
                ts: utc_midnight(2018, 6, 10),
            },
            Snapshot {
                name: "jun_11".to_string(),
                ts: utc_midnight(2018, 6, 11),
            },
            Snapshot {
                name: "jun_12".to_string(),
                ts: utc_midnight(2018, 6, 12),
            },
        ];
        let now = Utc.ymd(2018, 6, 12).and_hms(16, 0, 0);

        let expected: HashSet<String> = vec![
            "last_year".to_string(),
            "feb_1".to_string(),
            "mar_1".to_string(),
            "apr_1".to_string(),
            "may_1".to_string(),
            "jun_7".to_string(),
            "jun_8".to_string(),
            "jun_9".to_string(),
            "jun_10".to_string(),
            "jun_11".to_string(),
            "jun_12".to_string(),
        ].iter()
            .cloned()
            .collect();

        let filtered = keep_generations(&snapshots, &test_generations(), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn generations_3() {
        let snapshots = vec![
            Snapshot {
                name: "two_years_ago".to_string(),
                ts: utc_midnight(2016, 6, 1),
            },
            Snapshot {
                name: "last_year".to_string(),
                ts: utc_midnight(2017, 6, 1),
            },
            Snapshot {
                name: "jan_1".to_string(),
                ts: utc_midnight(2018, 1, 1),
            },
            Snapshot {
                name: "feb_1".to_string(),
                ts: utc_midnight(2018, 2, 1),
            },
            Snapshot {
                name: "mar_1".to_string(),
                ts: utc_midnight(2018, 3, 1),
            },
            Snapshot {
                name: "apr_1".to_string(),
                ts: utc_midnight(2018, 4, 1),
            },
            Snapshot {
                name: "may_1".to_string(),
                ts: utc_midnight(2018, 5, 1),
            },
            Snapshot {
                name: "jun_20".to_string(),
                ts: utc_midnight(2018, 6, 20),
            },
            Snapshot {
                name: "jun_21".to_string(),
                ts: utc_midnight(2018, 6, 21),
            },
            Snapshot {
                name: "jun_22".to_string(),
                ts: utc_midnight(2018, 6, 22),
            },
            Snapshot {
                name: "jun_23".to_string(),
                ts: utc_midnight(2018, 6, 23),
            },
            Snapshot {
                name: "jun_24".to_string(),
                ts: utc_midnight(2018, 6, 24),
            },
            Snapshot {
                name: "jun_25".to_string(),
                ts: utc_midnight(2018, 6, 25),
            },
            Snapshot {
                name: "jun_26".to_string(),
                ts: utc_midnight(2018, 6, 26),
            },
        ];
        let now = Utc.ymd(2018, 6, 26).and_hms(16, 0, 0);

        let expected: HashSet<String> = vec![
            "last_year".to_string(),
            "mar_1".to_string(),
            "apr_1".to_string(),
            "may_1".to_string(),
            "jun_20".to_string(),
            "jun_21".to_string(),
            "jun_22".to_string(),
            "jun_23".to_string(),
            "jun_24".to_string(),
            "jun_25".to_string(),
            "jun_26".to_string(),
        ].iter()
            .cloned()
            .collect();

        let filtered = keep_generations(&snapshots, &test_generations(), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn generations_after_long_break() {
        let snapshots = vec![
            Snapshot {
                name: "jan_1".to_string(),
                ts: utc_midnight(2018, 1, 1),
            },
            Snapshot {
                name: "feb_1".to_string(),
                ts: utc_midnight(2018, 2, 1),
            },
            Snapshot {
                name: "mar_1".to_string(),
                ts: utc_midnight(2018, 3, 1),
            },
        ];
        let now = Utc.ymd(2018, 12, 1).and_hms(12, 0, 0);
        let expected: HashSet<String> = vec!["jan_1".to_string(), "mar_1".to_string()]
            .iter()
            .cloned()
            .collect();

        let filtered = keep_generations(&snapshots, &test_generations(), &now);
        assert_eq!(filtered, expected);
    }

    #[test]
    fn archives_empty_input() {
        assert_eq!(parse_archives("".to_string()), Ok(Vec::new()));
    }

    #[test]
    fn archives_missing_timestamp() {
        let test_archives = indoc!(
            "archive-001\t2018-07-22 15:10:48
             archive-002
             archive-003\t2018-08-01 10:35:08"
        ).to_string();
        assert_eq!(parse_archives(test_archives).is_err(), true);
    }

    #[test]
    fn archives_valid() {
        let test_archives = indoc!(
            "archive-001\t2018-07-22 15:10:48
             archive-002\t2018-07-23 23:43:51
             archive-003\t2018-08-01 10:35:08"
        ).to_string();
        let expected = Ok(vec![
            Snapshot {
                name: "archive-001".to_string(),
                ts: Utc.ymd(2018, 7, 22).and_hms(15, 10, 48),
            },
            Snapshot {
                name: "archive-002".to_string(),
                ts: Utc.ymd(2018, 7, 23).and_hms(23, 43, 51),
            },
            Snapshot {
                name: "archive-003".to_string(),
                ts: Utc.ymd(2018, 8, 1).and_hms(10, 35, 8),
            },
        ]);

        assert_eq!(parse_archives(test_archives), expected);
    }
}
