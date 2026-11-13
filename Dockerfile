# revenue-in-flight in one image: the console, the observer and MCP (serve), the demo system (simulate), the router,
# the whole demo (demo), and the stdio MCP server (mcp).
#   docker build -t revenue-in-flight .
#   docker run --rm --network host revenue-in-flight simulate
#   docker run --rm --network host revenue-in-flight serve
#   claude mcp add revenue-in-flight -- docker run --rm -i --network host revenue-in-flight mcp
# Inside the image it listens on all interfaces, so MCP over HTTP stays off until RIF_MCP_TOKEN is set.

FROM node:22-bookworm-slim AS console
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY tsconfig.json ./
COPY ui ui
RUN npm run build

FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY catalog catalog
COPY kafka kafka
COPY proxy-filter proxy-filter
COPY router router
COPY contracts contracts
COPY simulator simulator
COPY observer observer
COPY server server
COPY cli cli
COPY docker-compose.yml ./
COPY stack/proxy.yaml stack/proxy.yaml
COPY --from=console /app/dist/ui dist/ui
RUN ./mvnw -q -B package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
ENV HOST=0.0.0.0
RUN useradd --system --uid 10001 rif
COPY --from=build /app/cli/target/revenue-in-flight.jar /app/revenue-in-flight.jar
USER rif
EXPOSE 8088 8090 8085
ENTRYPOINT ["java", "-jar", "/app/revenue-in-flight.jar"]
CMD ["serve"]
