use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

extern crate rustc_version;
use rustc_version::{version, version_meta, Channel, Version};

fn main() {
    let rust_version = version().expect("Could not get rustc version");
    let expected_major_version = 1;
    let expected_minor_version = 35;

    assert_eq!(rust_version.major, expected_major_version);

    if rust_version.minor < expected_minor_version {
        panic!(
            "You don't have the right Rust version installed. This build is fixed to: {}.{}.x",
            expected_major_version, expected_minor_version,
        )
    }
}
