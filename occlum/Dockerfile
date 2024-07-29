FROM ubuntu:20.04 as base
LABEL maintainer="Chunyang Hui <sanqian.hcy@antgroup.com>"

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends gnupg wget ca-certificates jq && \
    echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu focal main' | tee /etc/apt/sources.list.d/intel-sgx.list && \
    wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | apt-key add - && \
    echo 'deb [arch=amd64] https://occlum.io/occlum-package-repos/debian focal main' | tee /etc/apt/sources.list.d/occlum.list && \
    wget -qO - https://occlum.io/occlum-package-repos/debian/public.key | apt-key add - && \
    apt-get update && \
    apt-get install -y occlum-toolchains-gcc=0.27.0-1 occlum=0.30.1-1 occlum-sgx-tools=0.30.1-1 occlum-pal=0.30.1-1 occlum-runtime=0.30.1-1 libsgx-uae-service libsgx-dcap-ql libsgx-urts libsgx-dcap-default-qpl && \
    apt-get install curl binutils m4 make -y
ENV PATH="/opt/occlum/build/bin:/usr/local/occlum/bin:$PATH"
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*


FROM occlum/occlum:0.30.1-ubuntu20.04 as packager
WORKDIR /RUST    
RUN apt update && apt install -y clang
RUN rustup install nightly-2023-10-31-x86_64-unknown-linux-gnu && \
    rustup default nightly-2023-10-31-x86_64-unknown-linux-gnu && \
    rustup target add x86_64-unknown-linux-musl
COPY ./sgx-bool-registration-tool ./node/sgx-bool-registration-tool	
COPY ./src ./node/src
COPY ./Cargo.lock ./node/Cargo.lock
COPY ./Cargo.toml ./node/Cargo.toml
WORKDIR /RUST/node
RUN git config --system core.longpaths true
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN occlum-cargo build --release
COPY ./occlum/sgx_default_qcnl.conf /etc/sgx_default_qcnl.conf
COPY ./occlum/occlum_build.sh ./occlum_build.sh
COPY ./occlum/rust-demo.yaml ./rust-demo.yaml
RUN chmod +x ./occlum_build.sh && \
    ./occlum_build.sh && \
    cd occlum_instance &&\
    occlum package

FROM base as deployer
WORKDIR /root
COPY --from=packager /RUST/node/occlum_instance/occlum_instance.tar.gz .
RUN tar -xvzf occlum_instance.tar.gz
RUN rm occlum_instance.tar.gz
COPY --from=packager /etc/sgx_default_qcnl.conf /etc
RUN    mkdir -p /var/run/aesmd && \
    echo "LD_LIBRARY_PATH=/opt/intel/sgx-aesm-service/aesm nohup /opt/intel/sgx-aesm-service/aesm/aesm_service --no-daemon >/dev/null 2>&1 &" > /root/.bashrc

WORKDIR /root
