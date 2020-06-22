# Big Data project

This project is a mutlidiscipline work for Big Data course and Multivariable Analysis course.

The topic of research is to investigate factors of startups success (=how much investments do they raise)

### Dataset overview
Our data contains startups from AngelCo (https://angel.co/), a large database and social network for startups and founders.

They have only close commercial API, thats why a scraper is required to get the data.

### System overview
Our system consists of three parts:

- Scraper

We implemented scraper with BeautifoulSoup. It is fairly complex and unfortunately doesn't work - AngelCo closed their web access in the June (our data was scraped in May)

- Storage

Our storage is implemented with MongoDB. We could use other tools, but since our records are small nested documents, we considered Mongo to be appropriate for our usecase.

- Analytical platform

We provide Jupyter Notebook service with PySpark and Mongo connector, so that we can get insights from our data.  
In the [notebook](notebooks/analytics.ipynb) we show the graphs, that were actually included in the project presentation for Multivariate Analysis course.

