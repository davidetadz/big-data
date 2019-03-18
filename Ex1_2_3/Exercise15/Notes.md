# 2019_03_18
## Exercise 15

Like the previous exercise, but mapping each word with a unique integer.

- We can use the *setup* method of the *Reducer* class. We set a local var *nextInt = 0* used to keep track of the **state** represent a specific word. We increment value every time the system invoke *reduce* method.

- We **cannot** have two or more instances of the *Reducer* class because we use a local variable.
