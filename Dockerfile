# Use the .NET 6.0 SDK image to build the application
FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /app

# Copy the App directory
COPY App/ .

# Change to the directory containing the project file
WORKDIR /app/samples/Console.Sample

# Restore dependencies
RUN dotnet restore

# Build the application
RUN dotnet build -c Release --no-restore

# Publish the application
RUN dotnet publish -c Release -o /app/publish --no-restore

# Use the .NET 6.0 runtime image for the final image
FROM mcr.microsoft.com/dotnet/runtime:6.0 AS runtime
WORKDIR /app

# Copy the published application from the build stage
COPY --from=build /app/publish .

# Specify the entry point to run the application
ENTRYPOINT ["dotnet", "Console.Sample.dll"]