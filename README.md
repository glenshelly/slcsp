# slcsp-finder

This application determines the Second Lowest Cost Silver Plan for a specified set of zip codes.

Full information on the requirements can be found here: https://github.com/adhocteam/homework/tree/master/slcsp
 
### Assumptions regarding the requirements

This implementation takes the requirements at face value and optimizes for getting 
results for the 53 zip codes specified.  

If the real use case 
were to support an API that would return the slcsp for any given zip code, 
we would choose a different implementation approach.

### Likely Improvements in real life

Depending on the actual usuage of this functionality (one off script?  production-grade application?), the 
following improvements might be called for.

1. Use a properties file for such things as the input file location, or different names for the input files.
2. Add test cases, to run automatically before the package phase
1. Optimize for speed, if that's necessary
1. Use a real logging system

These (and other) improvements were not made to the current implemenation, in an effort to not over-engineer the application
without specified requirements.



# Building and Running

Details on running the application can be found in COMMENTS.md



