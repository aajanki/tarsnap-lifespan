# tarsnap_rotate

tarsnap_rotate expires [tarsnap](https://www.tarsnap.com/) backups according the [grandfather-father-son](https://en.wikipedia.org/wiki/Backup_rotation_scheme#Grandfather-father-son) backup rotation scheme. In other words, it keeps a specific number of daily, weekly, and monthly backups.

This application only expires old backups. You need to setup a script for making the backups separately. The names of the backups do not need to include the timestamp or follow any other specific template.

## Compiling

```
cargo build
```

## Running the tests

```
cargo test
```

## Usage

The backup rotation generations are given as command line arguments: `<number_of_backup_to_keep><interval_letter>`. `<interval_letter>` defines the rotation interval and must be one of is one of H = hourly, D = daily, W = weekly, M = monthly, Y = yearly, and `<number_of_backup_to_keep>` is the count of backups to keep for this generation. There can be one or more generation arguments.

Example: To keep 31 daily, 10 weekly and 12 monthly backups, and to print the expired backups, run:
```
target/debug/tarsnap_rotate -v 31D 10W 12M
```

## License 

* The MIT licence. See the [LICENSE](LICENSE) file.

## Developer contact

antti.ajanki@iki.fi
