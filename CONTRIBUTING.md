# Contributing guide

Thank you for your readiness to contribute to Near Analytics!  
Please read this file before the start, it will simplify your life and make the review process faster.

## Overall idea

We have [NEAR Indexer for Explorer](https://github.com/near/near-indexer-for-explorer) which collects the data streamed from Near Blockchain.
The resulting Indexer DB could be the best place for any sort of analytics, if only it were smaller.
`receipts` table has 125M or records today (2021-12-03), just `count(*)` takes 5 minutes.

We have to live with it, that's why we've introduced NEAR Analytics.
Every day we collect some useful values and store them in Analytics DB.
We had a small talk about the overall architecture, you can find it out [here](https://drive.google.com/file/d/17ONZ1Gg4HloADDoMm4cJDpvDlx1XLGio/view).

## Can I add my own statistics?

Sure, and we are ready to collect the data daily.
But, you need to design it properly.
We split the advice into the categories below.

### General

- The statistics should be general enough; it should be possible to reuse the collected data for other needs;
- Use intuitive naming; naming should suit well with the one we use at other Near projects;
- Write the documentation if it helps to understand the code; especially, write the documentation if you propose new entities.

### SQL

- The performance is super important. Please check the query plans, think how to improve any communication with Indexer DB;
- While creating a new table, use `NOT NULL` for all the columns you've added. If the column should be nullable, think twice, maybe the solution could be improved somehow;
- While creating a new table, think about data types and explain your choice in the comments. You could take inspiration from any of the existing tables;
- Do not compose SQL statements from the pieces. Even if you don't take the parameters from the user, it anyway leads us to the chance of SQL injection;
- Please format your SQL statements to simplify the reading. Be careful, IDEs will not do that since we store SQLs in strings. I usually write SQLs in a separate editor, format them, and only then copy-paste them into the project.

### Python code

- We use `black` to unify the formatting in the project. Run `black .` before any commit;
- Think twice before you add any new library; if it's required, don't forget to add it to `requirements.txt`;
- Try to follow Python common best practices.

## Final checklist before you open the PR

- [ ] The PR includes exhaustive explanation, what is being added, why, how do you plan to use this data;
- [ ] The PR includes performance measurements for the queries to the Indexer DB;
- [ ] The code is tested properly. Please set up your own environment and make end-to-end testing by computing the data for 4-5 days. I kindly suggest using Testnet for testing purposes, the average load there is lower;
- [ ] Review the code yourself before assigning the reviewer.
