#### Running The Application
This project is built with gradle using the application plugin. The only requirement is that Java 8 must be installed.
To run the reports, execute the following:

```./gradlew run```

By default, the application will look for the data files in the current directory and generate the
total_purchase_by_store report. To control what the application does, the following options can be used:

```
Usage: spark-engineer [options]

-l, --local <value>      run in local or cluster mode
-o, --output-format <value>
format of the output: csv, json, or parquet
-r, --report <value>     which report to run: category_popularity_by_store, device_by_year, total_purchase_by_store
-n, --num-months <value>
calculate based on n-many months in the past
```

Since gradle is using the application plugin, options can be added by using the --args option. For example:

```./gradlew run --args="-r category_popularity_by_store"```