//! Simple sequential MapReduce: `mr-sequential <app> <inputfiles...>`
//! (the Rust version of Go's src/main/mrsequential.go).
//!
//! It runs the maps and reduces one at a time, in a single process, and
//! is used by the tests to produce the correct output.

use mr::apps;
use mr::KeyValue;
use std::io::Write;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: mr-sequential <app> <inputfiles...>");
        std::process::exit(1);
    }
    let (mapf, reducef) = apps::lookup(&args[1]).unwrap_or_else(|| {
        eprintln!("cannot load app {}", args[1]);
        std::process::exit(1);
    });

    // Read each input file, pass it to Map, accumulate the intermediate
    // Map output.
    let mut intermediate: Vec<KeyValue> = Vec::new();
    for filename in &args[2..] {
        let content = std::fs::read_to_string(filename)
            .unwrap_or_else(|e| panic!("cannot open {}: {}", filename, e));
        intermediate.extend(mapf(filename, &content));
    }

    // A big difference from real MapReduce is that all the intermediate
    // data is in one place, intermediate[], rather than being partitioned
    // into NxM buckets.
    intermediate.sort_by(|a, b| a.key.cmp(&b.key));

    let oname = "mr-out-0";
    let mut ofile = std::fs::File::create(oname).unwrap();

    // Call Reduce on each distinct key in intermediate[], and print the
    // result to mr-out-0.
    let mut i = 0;
    while i < intermediate.len() {
        let mut j = i + 1;
        while j < intermediate.len() && intermediate[j].key == intermediate[i].key {
            j += 1;
        }
        let values: Vec<String> = intermediate[i..j].iter().map(|kv| kv.value.clone()).collect();
        let output = reducef(&intermediate[i].key, &values);

        // This is the correct format for each line of Reduce output.
        writeln!(ofile, "{} {}", intermediate[i].key, output).unwrap();
        i = j;
    }
    ofile.flush().unwrap();
}
