# revenue-in-flight in one image, the main way in: the app (the default), the stdio MCP server (mcp), and the sample
# data (try, on the stack `up` starts).
#   docker build -t revenue-in-flight .
#   docker run --rm -p 8088:8088 -v ~/.revenue-in-flight:/home/rif/.revenue-in-flight revenue-in-flight
#   claude mcp add revenue-in-flight -- docker run --rm -i --network host -v ~/.revenue-in-flight:/home/rif/.revenue-in-flight revenue-in-flight mcp
# Inside the image it listens on all interfaces, so the console asks for a login token (printed at start, or
# RIF_TOKEN) and MCP over HTTP stays off until RIF_MCP_TOKEN is set. The workspace is /home/rif/.revenue-in-flight:
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
RUN useradd --system --uid 10001 --create-home --home-dir /home/rif rif && mkdir -p /home/rif/.revenue-in-flight && chown -R rif /home/rif
COPY --from=build /app/cli/target/revenue-in-flight.jar /app/revenue-in-flight.jar
USER rif
ENV RIF_HOME=/home/rif/.revenue-in-flight
VOLUME /home/rif/.revenue-in-flight
EXPOSE 8088 8090 8085
ENTRYPOINT ["java", "-jar", "/app/revenue-in-flight.jar"]
CMD ["app", "--no-open"]
