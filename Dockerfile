FROM alpine:3.20

RUN apk add --no-cache \
	curl \
	tar \
	build-base \
	musl-dev

ENV ZIG_VERSION=0.15.1
ENV ZIG_URL=https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz

RUN curl -L "$ZIG_URL" -o zig.tar.xz \
	&& tar -xf zig.tar.xz \
	&& mv zig-x86_64-linux-${ZIG_VERSION} /opt/zig \
	&& rm zig.tar.xz

ENV PATH="/opt/zig:${PATH}"

WORKDIR /app
COPY . .

CMD ["zig", "build", "test", "test:verbose"]
