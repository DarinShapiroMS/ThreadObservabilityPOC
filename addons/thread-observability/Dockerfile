ARG BUILD_FROM
# hadolint ignore=DL3006
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8 \
    PIP_BREAK_SYSTEM_PACKAGES=1

# hassio-addons/base already provides bash + s6-overlay + bashio.
RUN apk add --no-cache \
    python3 \
    py3-pip \
    tzdata

WORKDIR /opt/thread-observability

COPY app/ /opt/thread-observability/app/
COPY rootfs/ /
COPY config.yaml /opt/thread-observability/config.yaml

RUN mkdir -p /data/thread-observability \
    && sed -i 's/\r$//' /etc/s6-overlay/s6-rc.d/core/run \
                        /etc/s6-overlay/s6-rc.d/mcp/run \
    && chmod 755 /etc/s6-overlay/s6-rc.d/core/run \
                 /etc/s6-overlay/s6-rc.d/mcp/run \
    && pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir /opt/thread-observability/app

# Entrypoint /init is inherited from hassio-addons base image.
