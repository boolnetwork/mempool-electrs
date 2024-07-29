#!/bin/bash
set -e

# initialize occlum workspace
rm -rf occlum_instance && mkdir occlum_instance && cd occlum_instance

occlum init && rm -rf image

new_json="$(jq '.resource_limits.kernel_space_stack_size = "30MB" |
                .resource_limits.kernel_space_heap_size = "256MB" |
                .resource_limits.kernel_space_heap_max_size = "4096MB" |
                .resource_limits.user_space_size = "1024MB" |
                .resource_limits.user_space_max_size = "4096MB" |
                .resource_limits.init_num_of_threads = 8 |
                .resource_limits.max_num_of_threads = 128 |
                .process.default_heap_size = "320MB" |
                .process.default_stack_size = "30MB" |
                .process.default_mmap_size = "2048MB" |
                .env.untrusted = ["EXAMPLE", "RUST_LOG", "UNTRUSTED_RPC_URL"] |
		.env.default = ["OCCLUM=yes", "HOME=/host", "RUST_LOG=info" ] |
                .metadata.debuggable = false |
                .feature.enable_edmm = false' Occlum.json)" && \
echo "${new_json}" > Occlum.json

copy_bom -f ../rust-demo.yaml --root image --include-dir /opt/occlum/etc/template

occlum build
