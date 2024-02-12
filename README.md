<div style="display: flex; justify-content: left; align-items: left;">
  <img src="media/bluescope3.svg" style="width: 100%; max-width: 360px;" alt="BlueScope">
</div>

Introduce science to SQL optimization.

# Description
Profiles the performance of SQL queries by running the N times and measuring the time it takes to execute 
with minimizing any external reasons such as compilation time, queue time e.t.c.

How many times SQL query will be run is determined by confidence level and margin of error. 

Compares results of 2 different queries using Student's t-test. Returns the result of the test (p-value) and the conclusion.

Table of currently supported databases:

| Data Base           | Current status |
|---------------------|---------------|
| Redshift Serverless | Supported  ✅   |
| Redshift Dedicated  | In development ⏳|
| BigQuery            | Planned       |
| PostgreSQL          | Planned       |
| MySQL               | Planned       |
# How to use
## CLI
CLI is in development. For now, you can use the package in your code.

## Package
To install package run
`pip install bluescope`

To use the package in your code:

```python
from bluescope import REDSHIFT_SERVERLESS
from bluescope import get_profiler
from bluescope.statsutils import find_significance

profiler_rs_cls = get_profiler(REDSHIFT_SERVERLESS)
profiler_rs = profiler_cls(host=*host*, port=*port*,
                           db=*db*, user=*user*,
                           password=*password*, agree=True)

pr_1 = profiler_rs.profile(*query_1*)
pr_2 = profiler_rs.profile(*query_2*)

p = find_significance(pr_1['mean'], pr_2['mean'],
                      pr_1['std'], pr_2['std'],
                      pr_1['sample_size'], pr_2['sample_size'])
```

# How to contribute
Any ideas, comments, code improvements are very welcome. 

To contribute to the project, please follow the steps:
1. Fork the repository
2. Create a new branch
3. Make your changes
4. Push your changes to your branch
5. Create a pull request
6. Wait for the review


# License
⚖️ GPL-3.0
# Contact
Maintainer: Mirzabekian Arkadii 📧 [Email](mailto:mirzabekian.arkadii@gmail.com)