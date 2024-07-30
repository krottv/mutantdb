fn main() {
    prost_build::Config::default()
        .bytes(&["."])
        .compile_protos(&["src/proto/meta.proto"], &["src/proto"])
        .unwrap();
}
