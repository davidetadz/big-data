# 2019_03_18
## Exercise 12

We should output records with a PM10 value **below** a user-provided threshold (inserted as argument of the program).

- We don't need a reducer phase.
> We have to set **number of reducers equal to 0**
- In order to insert a parameter, we have to send it to the instance of the mapper. So we need to set a *Property* in the *Configuration* object (after getting it, in the *Driver* class).
