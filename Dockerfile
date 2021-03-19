### --- Build mzbench artifacts --- ###
FROM erlang:22.1.8-alpine as build

ENV ORIG_PATH=$PATH
ENV CODE_LOADING_MODE=interactive

ENV MZBENCH_SRC_DIR /opt/mzbench_src
ENV MZBENCH_API_DIR /opt/mzbench_api
ENV HOME_DIR /root

WORKDIR $MZBENCH_SRC_DIR

# Install packages
RUN apk add --no-cache g++ make musl-dev zlib-dev py2-pip openssl git rsync

COPY . .

# Install Mzbench_api server, Node, Workers through default path
#  - Mzbench_api application would be installed to ${MZBENCH_API_DIR}
#  - Node application would be installed to ${HOME_DIR}/.local/share
RUN mkdir -p ${HOME_DIR}/.local/share/mzbench_workers \
    && pip install -r requirements.txt \
    && make -C ./server generate \
    && cp -R ./server/_build/default/rel/mzbench_api ${MZBENCH_API_DIR}/ \
    && make -C ./node install \
    && make -C ./node local_tgz \
    && ln -s ${HOME_DIR}/.local/cache/mzbench_api/packages/node-*_erts*.tgz ${HOME_DIR}/.local/cache/mzbench_api/packages/node-someversion-someos.tgz \
    && ln -s ${HOME_DIR}/.local/cache/mzbench_api/packages/node-*_erts*.tgz ${HOME_DIR}/.local/cache/mzbench_api/packages/node-$(git rev-parse HEAD)-someos.tgz

# Install Workers through default path
#  - Workers packages would be stored at ${HOME_DIR}/.local/share/mzbench_workers
RUN make -C ./server extract-workers


### --- Final small image --- ###
FROM erlang:20.3.8-alpine

# Config file can be added via runtime env MZBENCH_CONFIG_FILE:
# docker run --env MZBENCH_CONFIG_FILE=<path>

ENV ORIG_PATH=$PATH
ENV CODE_LOADING_MODE=interactive

# Refer https://kubernetes.io/docs/setup/release/notes/
ARG KUBECTL_VERSION=1.10.7
ARG KUBECTL_CHECKSUM=169b57c6707ed8d8be9643b0088631e5c0c6a37a5e99205f03c1199cd32bc61e

ENV MZBENCH_API_DIR /opt/mzbench_api
ENV HOME_DIR /root

COPY requirements.txt /tmp

# Install packages, install kubectl (refer https://kubernetes.io/docs/setup/release/notes/),
#    create ssh keys, make server.config
RUN apk add --no-cache libstdc++ git curl openssh openssh-server bash rsync net-tools py2-pip coreutils \
    && curl -O -L https://dl.k8s.io/v${KUBECTL_VERSION}/kubernetes-client-linux-amd64.tar.gz \
    && echo "${KUBECTL_CHECKSUM}  kubernetes-client-linux-amd64.tar.gz" | sha256sum -c - \
    && tar -xf kubernetes-client-linux-amd64.tar.gz \
    && mv kubernetes/client/bin/kubectl /usr/bin/kubectl \
    && rm -rf kubernete* \
    && chmod +x /usr/bin/kubectl \
    && mkdir -p /etc/mzbench ${HOME_DIR}/.local/share/mzbench_workers ${HOME_DIR}/.ssh \
    && ssh-keygen -A \
    && cp /etc/ssh/ssh_host_rsa_key ${HOME_DIR}/.ssh/id_rsa \
    && cat /etc/ssh/ssh_host_rsa_key.pub >> ${HOME_DIR}/.ssh/authorized_keys \
    && chmod 0600 ${HOME_DIR}/.ssh/authorized_keys \
    && echo "root:Docker!" | chpasswd && echo "PasswordAuthentication no" > /etc/ssh/sshd_config \
    && pip install -r /tmp/requirements.txt \
    && echo "[{mzbench_api, [ {auto_update_deployed_code, disable}, {custom_os_code_builds, disable}, {network_interface, \"0.0.0.0\"},{listen_port, 80}]}]." > /etc/mzbench/server.config

COPY --from=build $MZBENCH_API_DIR $MZBENCH_API_DIR
COPY --from=build ${HOME_DIR}/.local ${HOME_DIR}/.local

EXPOSE 80
WORKDIR $MZBENCH_API_DIR

CMD $MZBENCH_API_DIR/bin/mzbench_api foreground
