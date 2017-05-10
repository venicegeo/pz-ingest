To run the Loader service locally (without Vagrant), perhaps through Eclipse or through CLI, navigate to the project directory and run

    mvn clean install -U spring-boot:run

This will run the application on port 8084 of the machine.

NOTE: This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail. 
