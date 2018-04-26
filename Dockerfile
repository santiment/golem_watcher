FROM node:8.9.1-alpine AS builder

RUN apk update && apk upgrade && apk add --no-cache git python make g++

WORKDIR /app

COPY package.json /app/package.json
COPY package-lock.json /app/package-lock.json

RUN npm install --production

FROM node:8.9.1-alpine

WORKDIR /app

COPY --from=builder /app/node_modules /app/node_modules

COPY . /app

EXPOSE 3000

CMD npm start
