FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json ./
COPY public ./public
COPY seed ./seed
COPY src ./src

EXPOSE 4127

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:4127/api/health').then((r) => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "src/server.js"]
