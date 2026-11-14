# in-flight in one image, the main way in: the app (the default), the stdio MCP server (mcp), and the sample
# data (try, on the stack `up` starts).
#   docker build -t in-flight .
#   docker run --rm -p 8088:8088 -v ~/.in-flight:/home/in-flight/.in-flight in-flight
#   claude mcp add in-flight -- docker run --rm -i --network host -v ~/.in-flight:/home/in-flight/.in-flight in-flight mcp
# Inside the image it listens on all interfaces, so the console asks for a login token (printed at start, or
# INFLIGHT_TOKEN) and MCP over HTTP stays off until INFLIGHT_MCP_TOKEN is set. The workspace is /home/in-flight/.in-flight:
# mount it to keep profiles and flows. Secret references by env: need -e; by file:, the file mounted.

FROM node:22-bookworm-slim AS console
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY tsconfig.json ./
COPY ui ui
RUN npm run build

FROM eclipse-temurin:25-jdk AS build
WORKDIR /app
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY catalog catalog
COPY kafka kafka
COPY proxy-filter proxy-filter
COPY router router
COPY contracts contracts
COPY simulator simulator
COPY discovery discovery
COPY recovery recovery
COPY observer observer
COPY server server
COPY cli cli
COPY docker-compose.yml ./
COPY stack/proxy.yaml stack/proxy.yaml
COPY --from=console /app/dist/ui dist/ui
RUN ./mvnw -q -B package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
ENV HOST=0.0.0.0
RUN useradd --system --uid 10001 --create-home --home-dir /home/in-flight in-flight && mkdir -p /home/in-flight/.in-flight && chown -R in-flight /home/in-flight
COPY --from=build /app/cli/target/in-flight.jar /app/in-flight.jar
USER in-flight
ENV INFLIGHT_HOME=/home/in-flight/.in-flight
VOLUME /home/in-flight/.in-flight
EXPOSE 8088 8090 8085
ENTRYPOINT ["java", "-jar", "/app/in-flight.jar"]
CMD ["app", "--no-open"]
