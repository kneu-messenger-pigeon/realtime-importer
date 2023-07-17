# append to https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/Dockerfile
# see https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/.github/workflows/build.yaml#L20
ENV STORAGE_DIR /storage
RUN mkdir /storage && chmod 777 -R /storage
VOLUME /storage