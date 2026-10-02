FROM gcc:13
WORKDIR /app
# .dockerignore keeps keys, .git, and host-built binaries out of the context
COPY . .
RUN make build/event_engine && make test
CMD ["./build/event_engine"]
