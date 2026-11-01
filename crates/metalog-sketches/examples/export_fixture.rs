//! Export a Rust blob for independent Go compatibility verification.
fn main() {
    let destination = std::env::args().nth(1).expect("output path");
    let sketches = std::collections::HashMap::from([(
        "trace_id".to_owned(),
        include_bytes!("../../../tests/fixtures/bloom/snapshot.msgpack").to_vec(),
    )]);
    std::fs::write(
        destination,
        metalog_sketches::encode_ext_blob(&sketches).unwrap(),
    )
    .unwrap();
}
