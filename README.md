# [UTHealth COVID-19 Dashboard](http://texaspandemic.org)

Scraping, statistics, and diagnostics of Texas COVID-19 data

# Installation

1. Clone the repository
2. Install the requirements using `pip install -r requirements.txt` (preferably in a virtual environment)
3. Configure the environment variables in `.env` (see `.env.example` for an example)
4. Configure the dagster environment variables [see the dagster docs](https://docs.dagster.io/guides/running-dagster-locally)
5. Launch dagster in the cli using `dagster dev` in the root directory.
6. Navigate to `localhost:3000` in your browser to view the dagster dashboard.
7. Create a new run using Materialize All

# Future Work

- [ ] Dockerize the project
- [ ] Use a database to store the data instead of csv files
- [ ] Pull in additional sources
- [ ] Add a more robust testing suite & run diagnostics

Current ETL: 
![ETL](readme_images/etl.svg)