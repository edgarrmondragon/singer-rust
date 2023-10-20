use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;
use singer_rust::message::{BatchEncoding, MessageReader};

pub mod cli;

#[derive(Serialize)]
pub struct StateStats {
    count: u32,
    pub last_seen: Value,
}

#[derive(Serialize)]
pub struct Counter {
    schema: u32,
    record: u32,
    activate_version: u32,
    batch: u32,
}

impl Counter {
    fn new() -> Self {
        Counter {
            schema: 0,
            record: 0,
            activate_version: 0,
            batch: 0,
        }
    }
}

#[derive(Serialize)]
pub struct Stats {
    streams: HashMap<String, Counter>,
    pub state: StateStats,
}

impl Stats {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            state: StateStats {
                count: 0,
                last_seen: Value::Null,
            },
        }
    }
}

pub struct StatsReader {
    pub stats: Stats,
}

impl StatsReader {
    pub fn new() -> Self {
        Self {
            stats: Stats::new(),
        }
    }
}

impl Default for StatsReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageReader for StatsReader {
    fn process_record(
        &mut self,
        stream: String,
        _record: Value,
        _time_extracted: Option<String>,
        _version: u64,
    ) -> Result<(), serde_json::Error> {
        let counter = self
            .stats
            .streams
            .entry(stream)
            .or_insert_with(Counter::new);
        counter.record += 1;
        Ok(())
    }

    fn process_schema(
        &mut self,
        stream: String,
        _schema: Value,
        _key_properties: Vec<String>,
        _bookmark_properties: Vec<String>,
    ) -> Result<(), serde_json::Error> {
        let counter = self
            .stats
            .streams
            .entry(stream)
            .or_insert_with(Counter::new);
        counter.schema += 1;
        Ok(())
    }

    fn process_activate_version(
        &mut self,
        stream: String,
        _version: u64,
    ) -> Result<(), serde_json::Error> {
        let counter = self
            .stats
            .streams
            .entry(stream)
            .or_insert_with(Counter::new);
        counter.activate_version += 1;
        Ok(())
    }

    fn process_batch(
        &mut self,
        stream: String,
        _manifest: Vec<String>,
        _encoding: BatchEncoding,
    ) -> Result<(), serde_json::Error> {
        let counter = self
            .stats
            .streams
            .entry(stream)
            .or_insert_with(Counter::new);
        counter.batch += 1;
        Ok(())
    }

    fn process_state(&mut self, value: Value) -> Result<(), serde_json::Error> {
        self.stats.state.count += 1;
        self.stats.state.last_seen = value;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use serde_json::json;

    use singer_rust::MessageReader;

    use super::StatsReader;

    #[test]
    fn test_stats() {
        let mut reader = StatsReader::new();
        let file = File::open("../resources/example.singer").unwrap();
        let buffer = std::io::BufReader::new(file);
        reader.process_lines(buffer).unwrap();

        assert_eq!(reader.stats.streams.len(), 1);
        assert_eq!(reader.stats.streams["example"].schema, 1);
        assert_eq!(reader.stats.streams["example"].record, 2);
        assert_eq!(reader.stats.state.count, 1);
        assert_eq!(
            reader.stats.state.last_seen,
            json!({"bookmarks": {"example": {"updated_at": "2023-04-10T00:00:10Z"}}})
        );
    }
}
