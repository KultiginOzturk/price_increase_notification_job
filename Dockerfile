FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY server/ ./server/

ENV NODE_ENV=production

CMD ["node", "server/jobs/price_increase_notification_job/index.js"]
