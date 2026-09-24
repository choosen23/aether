FROM node:22-alpine AS deps
WORKDIR /app
COPY web/package.json web/package-lock.json ./web/
RUN npm --prefix web ci

FROM node:22-alpine AS build
WORKDIR /app
ARG NEXT_PUBLIC_API_HTTP_URL
ARG NEXT_PUBLIC_API_WS_URL
ARG NEXT_PUBLIC_MAP_STYLE_URL
ENV NEXT_PUBLIC_API_HTTP_URL=$NEXT_PUBLIC_API_HTTP_URL
ENV NEXT_PUBLIC_API_WS_URL=$NEXT_PUBLIC_API_WS_URL
ENV NEXT_PUBLIC_MAP_STYLE_URL=$NEXT_PUBLIC_MAP_STYLE_URL
COPY --from=deps /app/web/node_modules ./web/node_modules
COPY web ./web
RUN npm --prefix web run build

FROM node:22-alpine AS runtime
WORKDIR /app
COPY --from=build /app/web ./web
EXPOSE 3000
CMD ["npm", "--prefix", "web", "run", "start"]
