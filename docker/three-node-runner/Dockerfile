FROM alpine:3.19

RUN apk add --no-cache bash curl jq

WORKDIR /tests
COPY run.sh /tests/run.sh
ENTRYPOINT ["/bin/bash", "/tests/run.sh"]
