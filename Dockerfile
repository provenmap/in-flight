# revenue-in-flight: the app (serve), the demo system (simulate) and the stdio MCP server (mcp), in one image.
#   docker build -t revenue-in-flight .
#   docker run --rm --network host revenue-in-flight simulate
#   docker run --rm --network host revenue-in-flight serve
# Inside the image it listens on all interfaces, so MCP over HTTP stays off until RIF_MCP_TOKEN is set.

FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build && npm prune --omit=dev

FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production HOST=0.0.0.0
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/catalog ./catalog
COPY --from=build /app/docker-compose.yml ./
USER node
EXPOSE 8088 8090
ENTRYPOINT ["node", "dist/cli.js"]
CMD ["serve"]
