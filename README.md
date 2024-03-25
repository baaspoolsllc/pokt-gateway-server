<div align="center">
<img src="https://github.com/pokt-network/gateway-server/blob/main/docs/resources/gateway-server-logo.jpg" width="500" alt="POKT Gateway Server">
</div>

## What is POKT Gateway Server?

The POKT Gateway Server is a comprehensive solution designed to simplify the integration of applications with the POKT Network. Our goal is to reduce the complexities associated with directly interfacing with the protocol, making it accessible to a wide range of users, including application developers, existing centralized RPC platforms, and future gateway operators.

Learn more about the vision and overall architecture [overview](docs%2Foverview.md)

## Gateway Operator Quick Getting Started
To onboard the gateway server without having to dig deep, you can follow the [Quick Onboarding Guide](docs%2Fquick-onboarding-guide.md)


#### Interested in learning more? We have an abdundance of information in [docs](docs)
1. [Gateway Server Overview](docs%2Foverview.md)
2. [Gateway Server API Endpoints](docs%2Fapi-endpoints.md)
3. [Gateway Server System Architecture](docs%2Fsystem-architecture.md)
4. [Gateway Server Node Selection](docs%2Fnode-selection.md)
5. [POKT Primer](docs%2Fpokt-primer.md)
6. [POKT's Relay Specification](docs%2Fpokt-relay-specification.md)

## Creating a DB Migration
Migrations are like version control for your database, allowing your team to define and share the application's database schema definition.
Before running a migration make sure to install the go lang migration cli on your machine.
https://github.com/golang-migrate/migrate/tree/master/cmd/migrate
```sh
./scripts/migration.sh -n {migration_name}
```
This command will generate a up and down migration in `db_migrations`

## Applying a DB Migration
DB Migrations are applied upon server start, but as well, it can be applied manually through:
```sh
./scripts/migration.sh {--down or --up} {number_of_times} 
./scripts/migration.sh -d 1
./scripts/migration.sh -u 1
```

## Running Tests
Install Mockery with
```
go install github.com/vektra/mockery/v2@v2.40.1
```
You can generate the mock files through:
```sh
./scripts/mockgen.sh
```
By running this command, it will generate the mock files in `./mocks` folder.
Reference for mocks can be found here https://vektra.github.io/mockery/latest/

Run this command to run tests:
```sh
go test ./...
```

## Docker Compose
There is an all-inclusive docker-compose file available for usage [docker-compose.yml](docker-compose.yml)


## Contributing Guidelines
1. Create a Github Issue on the feature/issue you're working on.
2. Fork the project
3. Create new branch with `git checkout -b "branch_name"` where branch name describes the feature.
    - All branches should be based off `main`
3. Write your code
4. Make sure your code lints with `go fmt ./...` (This will Lint and Prettify)
5. Commit code to your branch and issue a pull request and wait for at least one review.
    - Always ensure changes are rebased on top of main branch.

---
## Project Structure

- **cmd:** Contains the entry point of the binaries
    - **gateway_server:** HTTP Server for serving requests
- **internal:** Shared internal folder for all binaries
- **pkg:** Distributable dependencies
- **docs:** Project documentation and specifications

## Core Project Dependencies
- [FastHTTP](https://github.com/valyala/fasthttp) for both HTTP Client/Server
- [FastJSON](https://github.com/pquerna/ffjson) for performant JSON Serialization and Deserialization
- Lightweight Pocket Client

We have implemented our own lightweight Pocket client to enhance speed and efficiency. Leveraging the power of [FastHTTP](https://github.com/valyala/fasthttp) and [FastJSON](https://github.com/pquerna/ffjson), our custom client achieves remarkable performance gains. Additionally, it has the capability to properly parse node runner's POKT errors properly given that the network runs diverse POKT clients (geomesh, leanpokt, their own custom client).

### Why It's More Efficient/Faster
1. **FastHTTP:** This library is designed for high-performance scenarios, providing a faster alternative to standard HTTP clients. Its concurrency-focused design allows our Pocket client to handle multiple requests concurrently, improving overall responsiveness.
2. **FastJSON:** The use of FastJSON ensures swift and efficient JSON serialization and deserialization. This directly contributes to reduced processing times, making our Pocket client an excellent choice for high-scale web traffic.

