FROM composer:2 AS vendor
WORKDIR /app
COPY composer.json composer.lock ./
RUN composer install --no-dev --no-interaction --prefer-dist --no-scripts --no-autoloader
COPY . .
RUN mkdir -p storage/framework/cache/data storage/framework/sessions storage/framework/views bootstrap/cache
RUN composer install --no-dev --no-interaction --prefer-dist --optimize-autoloader

FROM node:24-alpine AS assets
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY --from=vendor /app/vendor ./vendor
COPY resources ./resources
COPY vite.config.js ./
RUN npm run build

FROM php:8.4-cli-alpine
RUN apk add --no-cache libpq-dev \
    && docker-php-ext-install pdo_pgsql
RUN { \
        echo "post_max_size=64M"; \
        echo "upload_max_filesize=64M"; \
        echo "memory_limit=1536M"; \
        echo "max_execution_time=300"; \
        echo "max_input_time=300"; \
    } > /usr/local/etc/php/conf.d/ghostfrog-web.ini
WORKDIR /app
COPY --from=vendor /app /app
COPY --from=assets /app/public/build /app/public/build
RUN mkdir -p /app/storage/framework/cache/data /app/storage/framework/sessions /app/storage/framework/views /app/bootstrap/cache \
    && chmod +x /app/docker/start.sh \
    && chown -R www-data:www-data /app/storage /app/bootstrap/cache
EXPOSE 8000
CMD ["/app/docker/start.sh"]
