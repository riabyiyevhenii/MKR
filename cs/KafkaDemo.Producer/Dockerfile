# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY cs/KafkaDemo.Producer/KafkaDemo.Producer.csproj cs/KafkaDemo.Producer/
RUN dotnet restore cs/KafkaDemo.Producer/KafkaDemo.Producer.csproj

COPY cs/KafkaDemo.Producer/. cs/KafkaDemo.Producer/
RUN dotnet publish cs/KafkaDemo.Producer/KafkaDemo.Producer.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime
WORKDIR /app
COPY --from=build /app .

ENV BOOTSTRAP_SERVERS=kafka:29092
ENTRYPOINT ["dotnet", "KafkaDemo.Producer.dll"]
