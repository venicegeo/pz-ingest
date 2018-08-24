# pz-ingest

The Ingest component is the internal component that handles the loading of spatial data. This component is capable of referencing data held in external locations, such as another accessible S3 file store; or loading data specified by the user to be stored directly within Piazza. The Ingest component receives RabbitMQ messages from the Gateway, with the information as to the file to be stored. It then inspects the data to validate and populate metadata fields (such as Area of Interest) and then stores this metadata within the Piazza PostgreSQL instance.

***
## Requirements
Before building and/or running the pz-search-query service, please ensure that the following components are available and/or installed, as necessary:
- [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (JDK for building/developing, otherwise JRE is fine)
- [Maven (v3 or later)](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- [PostgreSQL](https://www.postgresql.org/download)
- [RabbitMQ](https://www.rabbitmq.com/download.html)
- [Vagrant](https://www.vagrantup.com/docs/installation/) (for running local ElasticSearch)
- Access to Nexus is required to build

Ensure that the nexus url environment variable `ARTIFACT_STORAGE_URL` is set:

	$ export ARTIFACT_STORAGE_URL={Artifact Storage URL}

For additional details on prerequisites, please refer to the Piazza Developer's Guide [Core Overview](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/02-pz-core.md) or [Piazza Ingest](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/10-pz-ingest.md) sections. Also refer to the [prerequisites for using Piazza]https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/03-jobs.md) section for additional details.


***
## Setup, Configuring, & Running
### Setup
Create the directory the repository must live in, and clone the git repository:

    $ mkdir -p {PROJECT_DIR}/src/github.com/venicegeo
	$ cd {PROJECT_DIR}/src/github.com/venicegeo
    $ git clone git@github.com:venicegeo/pz-ingest.git
    $ cd pz-ingest

>__Note:__ In the above commands, replace {PROJECT_DIR} with the local directory path for where the project source is to be installed.

### Configuring
As noted in the Requirements section, to build and run this project, RabbitMQ and PostgreSQL are required. The `src/main/resources/application.properties` file controls URL information for connection configurations.

### Running

To run the Loader service locally (without Vagrant), perhaps through Eclipse or through CLI, navigate to the project directory and run

    $ mvn clean install -U spring-boot:run

This will run the application on port 8084 of the machine.

>__NOTE:__ This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail.

### Running Unit Tests

To run the Piazza ingest unit tests from the main directory, run the following command:

	$ mvn test
