# syntax=docker/dockerfile:1
# Debian-based (glibc) image: required by the pulsar-client native binary
FROM node:22-bookworm-slim
WORKDIR /app
COPY package*.json ./
# Optional extra root CA (secret "extra_ca"), for machines where antivirus or a
# proxy intercepts HTTPS. Used only during npm ci and never stored in the image.
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/extra_ca; fi; \
    npm ci --omit=dev
COPY . .
EXPOSE 4000
CMD ["node", "server.js"]
