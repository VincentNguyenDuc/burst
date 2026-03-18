fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/burst/v1/control.proto",
                "proto/burst/v1/job.proto",
                "proto/burst/v1/worker.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile proto files");
}
