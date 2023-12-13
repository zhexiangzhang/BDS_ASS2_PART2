using Client.Stream;

// Before running the application, install the correct version of .NET SDK
// The version is specified in the "global.json" file

// Need to start the Orleans server and the kafka service first
// To run kafka service on your PC, follow the steps:
// (1) install Docker Desktop (https://www.docker.com/products/docker-desktop/)
// (2) run "docker-compose up -d" to start the container which will run the kafka service
//     - reference: https://developer.confluent.io/quickstart/kafka-docker/
//     - need to have internet to download the kafka image
//     - the docker-compose.yml file is in the folder "BDS-Programming-Assignment"
var streamClient = new StreamClient();
await streamClient.RunClient();
return;