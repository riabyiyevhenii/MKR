# syntax=docker/dockerfile:1
# Build stage: SDK з усім потрібним для restore + publish
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Спершу копіюємо тільки .csproj — щоб шар з restore кешувався, поки залежності не змінились
COPY cs/KafkaDemo.Consumer/KafkaDemo.Consumer.csproj cs/KafkaDemo.Consumer/
RUN dotnet restore cs/KafkaDemo.Consumer/KafkaDemo.Consumer.csproj

# Тепер решта вихідників і публікація
COPY cs/KafkaDemo.Consumer/. cs/KafkaDemo.Consumer/
RUN dotnet publish cs/KafkaDemo.Consumer/KafkaDemo.Consumer.csproj \
    -c Release -o /app /p:UseAppHost=false

# Runtime stage: тонкий образ без SDK
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime
WORKDIR /app
COPY --from=build /app .

# Дефолти для запуску в одній мережі з контейнером kafka.
ENTRYPOINT ["dotnet", "KafkaDemo.Consumer.dll"]
