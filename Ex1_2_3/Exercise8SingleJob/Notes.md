# 2019_03_18
## Exercise 8
## Single-Job solution

Hint: there're only 12 months. We need to think about using another key, so using only the year (for each input line). As value, we need the income and at the same time the month, in order to compute infos about monhts.
- Class *MonthIncome* created in order to store both of them as value.

> (2015, i = 1000 - m = 11)
> (2015, i = 1305 - m = 11)
> (2015, i = 500  - m = 12)
> (2015, i = 750  - m = 12)

Values (on the left) are the one received by the reducer when invoked for year 2015. We can use an HashMap (*totalMonthIncome*), in which each entry is characterized by month as key and income as value. When adding a new value, we should sum the existing income in the HashMap with the new one and update value in the HashMap.

At the end of the loop, we have the total income for each month in the HashMap.
Then, we have to compute the average income.

We can do everything in one single job because
- We have to store the information in the HashMap, with a maximum size of 12 for each year. It's **very small** so it can be stored in the main memory.
- Data types are almost the same in outputs

Usually, we do two jobs but this is a very particular problem.
