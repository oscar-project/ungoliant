FROM rust:latest AS build
COPY Cargo.toml Cargo.toml
COPY src/ src/
COPY tests/ tests/
COPY benches/ benches/
COPY README.md README.md

# Add deps for kenlm
RUN apt update && apt install -y libboost-all-dev libeigen3-dev cmake clang
RUN cargo build --all-features --release
RUN ls target
RUN ls target/release

FROM alpine AS dl
ARG model_url=https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
#TODO blocklist commit?

# get git
RUN apk add wget git unzip

# Kenlms too big?

# get langid
RUN wget -O langid.bin $model_url

# get blocklist
RUN wget https://github.com/olbat/ut1-blacklists/archive/refs/heads/master.zip 
RUN unzip master.zip
RUN mv ut1-blacklists-master ut1-blacklists

# decompress the biggest one
RUN gzip -d ut1-blacklists/blacklists/adult/domains.gz

# extract blocklist commit id
#RUN git rev-parse HEAD > ut1-blacklists-commitid.txt

# find something lighter?
FROM debian

# copy binary
COPY --from=build target/release/ungoliant /bin/ungoliant
RUN ls /bin/

# copy model 
COPY --from=dl langid.bin /langid.bin
COPY --from=dl ut1-blacklists/blacklists/ /blocklists/

# create volumes for shards and corpus output
VOLUME /shards
VOLUME /kenlm
VOLUME /output

RUN ls


ENTRYPOINT ["/bin/ungoliant"]

#CMD ["pipeline", "--foo"]
CMD ["pipeline", "--domain-blocklists", "/blocklists/", "--kenlms-path", "/kenlm", "--lid-path", "langid.bin", "--split_size", "10000", "--comp", "/shards", "/output"]
