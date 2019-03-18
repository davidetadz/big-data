# 2019_03_18
## Exercise 9

If the word is available in the HashMap we increment the number of occurencies in the HashMap. The *map* function will be invoked multiple times.
Then, again in the *Mapper*, we ouput a word at a time with the associated count.

**Note**
In this case a combiner will be useless, because we already output data as combined.