1. Discuss maven and where to get it...

To run the Loader service locally (without Vagrant), perhaps through Eclipse or through CLI, navigate to the project directory and run

    mvn clean install -U spring-boot:run
    
** Other note.  Access to Nexus is required to build (See the pom).  Switch to use other springboot maven library. 

To build and run this project, software such as RabbitMQ and PostgreSQL are required.  For details on these prerequisites, refer to the
[Piazza Developer's Guide](https://pz-docs.geointservices.io/devguide/index.html#_piazza_core_overview).

This will run the application on port 8084 of the machine.

NOTE: This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail. 

Running

1. Discuss vagrant and where to get it/learn about
2. Add pointer to dependencies which will be in pz-docs new section

