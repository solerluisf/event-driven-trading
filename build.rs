// build.rs - Build script for Windows linkage
// Links Windows security libraries needed by zmq-sys

fn main() {
    #[cfg(target_os = "windows")]
    {
        println!("cargo:rustc-link-lib=advapi32");
    }
}
