
# Building and Running



## Assumptions 

This application uses Java 8, and assumes it is available on the computer running it.

The instructions below for building the application assume that Maven is available.
 


## Building the application with Maven

Run the maven install phase from the root of the application

> mvn package

This will create a slcsp_finder.jar in ./target/


## Running the application

To run the application, execute the slcsp_finder jar with a single parameter, as follows: 

```
> java  -jar  target/slcsp_finder.jar  <the-single-parameter>
```
The value of this single parameter is the location (relative or absolute) of the 3 input files. 

For example, if the input files were in the '/data" beneath the directory you're running the application in, 
 run the application as follows: 

```
> java -jar target/slcsp_finder.jar ./data 
```

The results will overwrite the original values in the slcsp.csv file in that same directory.
