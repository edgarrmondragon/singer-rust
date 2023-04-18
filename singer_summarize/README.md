# singer-summarize

A POC Singer target that summarizes a tap's output.

## Installation

### From PyPI

I recommend using [pipx](https://pypa.github.io/pipx/) to install this package:

```shell
pipx install singer-summarize
```

## Build

### Binary

```shell
cargo build -p singer_summarize --release
```

This will create a binary at `target/release/singer-summarize`.

### Python

```shell
maturin build --release
```

This will create a Python wheel at `target/wheels/`.

## Usage

Copy the binary to your `PATH`:

```shell
cp target/release/singer-summarize ~/.local/bin
```

Test it with a Singer tap:

```console
$ tap-exchangeratesapi -c config.json | singer-summarize
{
  "streams": {
    "exchange_rate": {
      "schema": 1,
      "record": 100,
      "activate_version": 0,
      "batch": 0
    }
  },
  "state": {
    "count": 1,
    "last_seen": {
        "start_date": "2020-01-01",
    }
  }
}
```
