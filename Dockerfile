# -----------------------------------------------
# Stage 1: Builder (install dependencies and build)
# -----------------------------------------------
FROM node:23-alpine AS builder

# Create and set working directory
WORKDIR /usr/src/picoWeatherCollector

# Copy package files first for better layer caching
COPY package*.json ./

# Install production dependencies (clean cache afterwards)
RUN npm ci --only=production && \
    npm cache clean --force

# Copy application source
COPY . .

# -----------------------------------------------
# Stage 2: Runtime (production-optimized image)
# -----------------------------------------------
FROM node:23-alpine

# Install dumb-init for proper signal handling
RUN apk add --no-cache dumb-init && \
    # Create non-root user for security
    adduser -D weatherCollector && \
    # Create app directory and set permissions
    mkdir -p /usr/src/picoWeatherCollector && \
    chown weatherCollector:weatherCollector /usr/src/picoWeatherCollector

WORKDIR /usr/src/picoWeatherCollector

# Copy from builder stage
COPY --from=builder --chown=weatherCollector:weatherCollector /usr/src/picoWeatherCollector .

# Environment variables (non-sensitive config only)
ENV NODE_ENV=production \
    PORT=5000

# Expose application port
EXPOSE ${PORT}

# Switch to non-root user
USER weatherCollector

# Entrypoint for proper signal handling
ENTRYPOINT ["dumb-init", "--"]

# Start command (direct node execution for production)
CMD ["node", "server.js"]
