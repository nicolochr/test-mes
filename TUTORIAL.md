# DBT Tutorial

- [DBT Tutorial](#dbt-tutorial)
  - [Description](#description)
  - [Verify the information](#verify-the-information)
  - [Add the first sources](#add-the-first-sources)
  - [Organize your dbt project](#organize-your-dbt-project)
  - [Write your first work dbt models from the source](#write-your-first-work-dbt-models-from-the-source)
    - [Models' config blocks](#models-config-blocks)
    - [Source References](#source-references)
    - [dbt\_utils](#dbt_utils)
    - [Run your first models](#run-your-first-models)
  - [Create Pub Models and Lint your models](#create-pub-models-and-lint-your-models)
  - [Lint your models with SQLFluff](#lint-your-models-with-sqlfluff)
  - [Test and Document your models](#test-and-document-your-models)
    - [Basic of testing with dbt](#basic-of-testing-with-dbt)
    - [Documentation principles](#documentation-principles)
  - [Final Word](#final-word)
  - [Next steps:](#next-steps)

## Description

This guide is designed for users discovering dbt and data products.

Before starting, you should have:
- [ ] access to your Data Product's github repo
- [ ] access to your Data Product's dev GCP project
  - run `gcloud config set project <YOUR_PROJECT_NAME>`
  - run `gcloud auth application-default login` to authenticate to GCP
- [ ] python and dbt installed

:question: reach out to the platform team if you have any questions here!

This tutorial will show you the first important steps to get your first data out of dbt. You will learn to:

1. Understand the different generated files
2. Verify that the information at the dbt project creation is accurate
3. Add your first source
4. Get your dbt project organized
5. Write your first dbt models from the source
6. Run your models locally
7. Create child models referencing other dbt models
8. Implement your first dbt tests
9. Document your dbt models
10. Run and test your models locally

At the end of this tutorial, you will have:
- [ ] a running dbt project,
- [ ] linted with good practices
- [ ] that is tested
- [ ] with results available in BigQuery

## Verify the information

The Data Platform Team has created this github repo based on the inputs you filled. Before working on your dbt models, let's check the following:

- [ ] Go to the [prodspec file](./prodspec.yml) and verify the following inputs are as expected:
  - [ ] `domain`, `brand` and `source`: they will be used to generate your project name, your datasets in GCP and the labels of your BQ tables
  - [ ] Owner name and id: the id is the name of the created GCP group
  - [ ] FullyQualifiedName: `<brand>_<?source>_<productName>` This will mainly be used in the CI
  - [ ] Check your source name (which will be used in dbt) and value (the GCP project ID where the data you want to query is stored)
- [ ] Go to the [profiles YAML File](./profiles.yml) and identify the 3 records in the output section
  - dev will be used when you run dbt locally
  - test will be used within the PR CI jobs
  - prod will be used during production runs
  - verify that the GCP project IDs are correct
  - The datasets could be adjusted to your needs or team's decisions

## Add the first sources

The first step to start coding is to add sources to your dbt project. Sources make it possible to name and describe the data you will use in your data product. By declaring these tables as sources in dbt, you can then:

- select from source tables in your models using `{{ source() }}`, helping define the lineage of your data
- test your assumptions about your source data
- calculate the freshness of your source data

Go to [the sources file created here](./dbt/models/sources.yml) which is well documented and will help you understand how this works.

## Organize your dbt project

This may change in the future but we recommend following this approach:
| Category | Description                                             | Directory |
|----------|---------------------------------------------------------|-----------|
| Work     | Contains models which clean and standardize data.<br>Work models are not supposed to be exposed to other data products or other external product (bi tools...)     | wrk       |
| Public   | Contains models which are supposed to be shared with other data products or other external tools | pub       |

## Write your first work dbt models from the source

This is the first step to follow. We have created 2 models [`wrk_cycle_hire`](./dbt/models/dbt_tutorial/wrk/wrk_cycle_hire.sql) and [`wrk_cycle_stations`](./dbt/models/dbt_tutorial/wrk/wrk_cycle_stations.sql) to show you how to super easily create dbt models.

3 main pieces of information available are:
- config block
- source references
- dbt_utils functions especially surrogate keys

### Models' config blocks

They are positionned at the beginning of the models and allow you to overwrite some predefined configuration of the model.

Mostly, they are used in dbt to define the model materialization: this will often be a view or a table.

**Definitions:**
- **table**: In simplest terms, a table is the direct storage of data in rows and columns. Think excel sheet with raw values in each of the cells.
- **view**: A view is a virtual table defined by a SQL query. A view's results are created by executing the query at the time that the view is referenced in a query.

### Source References

In order to query data from a source you use the `{{ source(name, table_name) }}` syntax:
```sql
-- london_bicycles is defined as the source name sources.yml file
-- cycle_hire is the name of one of the tables defined in the sources.yml file
SELECT *
FROM {{ source('london_bicycles', 'cycle_hire') }} AS exchange_rate_plan
```

### dbt_utils

dbt allows users to use and create packages. You can think of packages as libraries of models, macros and other dbt objects that can be reused across several dbt projects. A package particularly useful is the dbt_utils package which contains very cool macros and tests, for instance the following 2:

- [generate_surrogate_key](https://github.com/dbt-labs/dbt-utils#generate_surrogate_key-source)
- [safe_divide](https://github.com/dbt-labs/dbt-utils#safe_divide-source)

- [ ] Install the dbt_utils package right now by running `dbt deps`.

### Run your first models

To do so, you can just type from your terminal type:
```bash
dbt run --select wrk_cycle_stations
```

## Create Pub Models and Lint your models

As described above, pub models are models which will have heavily transformed and joined data. Here is an example of [a pub model](./dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql).

Referencing an existing dbt model is very similar to referencing a source. Instead of `{{ source(...) }}`, the `{{ ref() }}` macro is used:

select from existing dbt model:
```sql
SELECT *
FROM {{ ref('wrk_cycle_hire') }}
```

select from source:
```sql
SELECT *
FROM {{ source('london_bicycles', 'cycle_hire') }}
```

Using `{{ ref(...) }}` allows dbt to understand dependencies between your models. As such, you can run all parents of a model (i.e. all "upstream" models), and the model itself, with a simple command:
```bash
dbt run --select +pub_weekly_trips_per_area
```

## Lint your models with SQLFluff

We also have a linter available in your dbt project called `SQLFluff`. Take a look at [`pub_weekly_trips_per_area.sql`](./dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql). It has several issues: inconsistent formatting, whitespaces, weird casing. The linter will catch a lot of those issues!

To test it, you can run in your model:
```bash
sqlfluff lint dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql
```

Use the `fix` command in order to fix your code following the linting rules the team set up in [the config file here](./.sqlfluff):
```bash
sqlfluff fix dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql
```

It fixed over 25 issues for you, how great is that?

Notice that not all issues can be automatically fixed (you will see a ```[2 unfixable linting violations found]``` message in your terminal output after running the `fix` command). If you re-run the `lint` command you can see
what these 2 issues are and where they occur. Your terminal output should show:

```bash
== .../dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql] FAIL
L:  25 | P:   5 | L034 | Select wildcards then simple targets before calculations
                       | and aggregates.
L:  45 | P: 102 | L016 | Line is too long.
All Finished 📜 🎉!
```
To fix these violations manually, edit line 45 to be shorter by removing the leading space characters and re-order the columns selected starting line 25 so that columns with calculations are selected after those without, i.e.

```sql
    SELECT
        start_station.station_area   AS start_area,
        DATE_TRUNC(start_date, WEEK) AS rental_week,
        COUNT(DISTINCT rental_id)    AS count_rental,
        AVG(duration)                AS avg_duration_in_s
```

After making these changes, if you re-run the `lint` command you should have no violations. :tada: Congrats you have just linted [`pub_weekly_trips_per_area.sql`](./dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql)!

## Test and Document your models

### Basic of testing with dbt

This is one of the killer features of dbt: turnkey testing and technical documentation solution. This is vital to your Data Products to have a clear testing strategy implemented within dbt.

The demo models showcase the most generic tests that need to be implemented first throughout your Data Products in order to stay on top of potential problems, ship new models with confidence and detect anomalies before everyone.

The [wrk_cycle_hire yaml file](./dbt/models/dbt_tutorial/wrk/wrk_cycle_hire.yml) show you some generic column tests from dbt:

- **not_null**: test that a specific column contains no NULL values
- **unique**: test that a specific column has no duplicate values
- **relationships**: test that all values of a specific column in the tested model are also in the column of another dbt model.

Examples of theses are already defined, run `dbt test` to see how it works.
```bash
dbt test --select wrk_cycle_hire wrk_cycle_stations
```

A good practice is to always add tests to your public model. Run the following:
```bash
dbt test --select pub_weekly_trips_per_area
```
Oh no, the tests fail! Looking at the expected values, we only want `station_area` that begin with the letter 'F'. Find the line that was commented out by mistake in [`pub_weekly_trips_per_area.sql`](./dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.sql) and un-comment it.

Nice, now run `dbt build --select pub_weekly_trips_per_area` (`dbt build` will run both models and tests for you). It should now work!

### Documentation principles

We recommend to document as much as possible all your models and columns. This dbt doc will be propagated as well to bigquery and to the next datacatalog chosen within Kering. I keep referencing [this dbt article called Sharing Knowledge](https://www.getdbt.com/blog/scaling-knowledge/) which should convince you on why you should document dbt models and key columns.

In order to easily document you can create a [MARKDOWN file like this pub_doc.md](./dbt/models/dbt_tutorial/pub/pub_doc.md) where you can write doc blocks which could be detailed and that you will then reference in [the yaml page like here](./dbt/models/dbt_tutorial/pub/pub_weekly_trips_per_area.yml)

```yaml
models:
- name: pub_weekly_trips_per_area # name of the model
  description: '{{ doc("pub_weekly_trips_per_area") }}'
```

You can now run the following command:
```bash
dbt docs generate && dbt docs serve
```

You will then generate and create locally a small documentation site that you can browse. You will there easily find the models you created with their documentation, the lineage of your project and the different tests run.

## Final Word

You can now go to Bigquery to see the resulting models and play around. You have now completed the tutorial :medal:

## Next steps

1. If you want to know more about the Data Platform, here are several links that will help you learn more about what we are building:
   1. PR Workflow (you can even open a PR where you build a new pub model to transpose your learnings)
   2. [SQL Style Guide and convention](https://confluence.keringapps.com/x/ci2FKg)
   3. how to: shape your data product
2. More advanced dbt documentation [is available here](https://confluence.keringapps.com/x/WyHgKg) with some in-house content (videos and how-tos) and also external resources
