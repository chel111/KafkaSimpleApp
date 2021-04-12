FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

ARG TOPIC
RUN echo ${TOPIC}

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["src/Kafka.Consumer/Kafka.Consumer.csproj", "src/Kafka.Consumer/"]
RUN dotnet restore "src/Kafka.Consumer/Kafka.Consumer.csproj"
COPY . .
WORKDIR "/src/src/Kafka.Consumer"
RUN dotnet build "Kafka.Consumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Kafka.Consumer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT dotnet Kafka.Consumer.dll ${TOPIC}