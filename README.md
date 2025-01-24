# SCALABLE CAPITAL CHALLENGE

An ETL Pipeline which reads in a JSON file with music data, and puts it into a set of DuckDB tables on disk
<img width="713" alt="Screen Shot 2025-01-24 at 10 19 24 AM" src="https://github.com/user-attachments/assets/2def4ce9-016e-42d9-9820-5f1c896b7537" />

## Prerequisites
- Docker
## Instructions
- Simply run make build to run the entire ETL pipeline.
## Design Decisions
- There are several design considerations that have to be made for this example problem that are probably less likely to occur in enterprise settings. For me these included
  - Not having access to a Kubernetes cluster or Airflow to run this job, as Kubernetes is set up to handle job orchestration. 
  - Rather than pre-process the data myself, I relied on DuckDB's built in transformation functions via Arrow to use several threads to read the jsonf file into memory. It runs quite quickly
  - On that note - I also chose not to use an ORM here like SQLAlchemy because it wouldn't actually do much in terms of protecting the database or reducing clutter in the repository
  - I decided to seperate SQL as being the business layer of abstraction for these questions. Therefore, validation is also done by reading in these same files and comparing results
  - Here, I am using pandas but in an enterprise setting, using a distributes system like spark (especially as a parquet as the rows of this dataset do not rely on each other) would be an excellent choice for larger files
  - We do have schemas that can be changed, but implementing something like alembic would make database migrations very easy
  - I am, in this example, offloading some operations on the simulated database because it is faster and far cleaner this way. In reality, we would have to be very aware of how much our cloud computing costs impact our budget
  - In a proper hosted cloud provider, we would probably be able to hook up an S3 bucket with SNS messages to trigger this job (lambdas, kafka, however we wanted to trigger it) - and we would in an ideal scenario also have statistics about how the runs are performing via something like prometheus, or directly in Airflow by looking at DAG statistics
  - We do not  perform validation here as Arrow (as it is set up in this project) will automatically reject rows that do not conform to our schema. Having some more validation before and after (assuming parallel computing is possible) would not slow us down too much. Post validation would also definitely be possible
