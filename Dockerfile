# ---- build stage ----
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
# Commit package-lock.json and switch to `npm ci` for reproducible builds.
RUN npm install
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- runtime stage ----
FROM node:20-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist
USER node
EXPOSE 3000 9101
CMD ["node", "dist/api/server.js"]
