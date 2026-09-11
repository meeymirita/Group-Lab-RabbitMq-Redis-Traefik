# Group Lab: RabbitMQ, Redis, Traefik

Сборный репозиторий с лабораторными работами. Каждая работа подключена как git submodule в отдельной папке. Репозиторий будет пополняться новыми работами.

## Работы

| Папка | Описание | Репозиторий |
|---|---|---|
| [`rabbitmq`](rabbitmq) | Лабораторная по RabbitMQ (Docker Compose, Laravel-приложение) | [Rabbitmq-laboratornaya-](https://github.com/meeymirita/Rabbitmq-laboratornaya-) |
| [`redis`](redis) | Лабораторная по Redis | [Redis-Lab-laboratornaya-](https://github.com/meeymirita/Redis-Lab-laboratornaya---) |
| [`traefik`](traefik) | Лабораторная по Traefik | [Traefik-Lab-laboratornaya-](https://github.com/meeymirita/Traefik-Lab-laboratornaya--) |

## Клонирование

Репозиторий использует submodule, поэтому клонировать нужно с флагом `--recurse-submodules`:

```bash
git clone --recurse-submodules https://github.com/meeymirita/Group-Lab-RabbitMq-Redis-Traefik.git
```

Если репозиторий уже склонирован без этого флага:

```bash
git submodule update --init --recursive
```

## Добавление новой работы

```bash
git submodule add <url-репозитория-лабы> <папка>
git commit -m "Add <название> lab"
```
