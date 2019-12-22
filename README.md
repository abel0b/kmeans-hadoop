# k-means clustering
Implementation of kmeans clustering with mapreduce.

## Usage
```
yarn jar kmeans.jar [INPUT_FILE] [OUTPUT_FILE] [K] [COLUMN] ...
```

`INPUT_FILE` Text file, data separated with ','
`OUTPUT_FILE` Text file that will be created
`K` Number of clusters
`COLUMN` Columns to read in text file, see example

## Build
```shell
mvn compile
mvn package
```

## Example
Dataset about cities (Country,City,AccentCity,Region,Population,Latitude,Longitude)
```
ad,aixas,Aix�,06,,42.4833333,1.4666667
ad,aixirivali,Aixirivali,06,,42.4666667,1.5
ad,aixirivall,Aixirivall,06,,42.4666667,1.5
ad,aixirvall,Aixirvall,06,,42.4666667,1.5
ad,aixovall,Aixovall,06,,42.4666667,1.4833333
ad,andorra,Andorra,07,,42.5,1.5166667
ad,andorra la vella,Andorra la Vella,07,20430,42.5,1.5166667
ad,andorra-vieille,Andorra-Vieille,07,,42.5,1.5166667
ad,andorre,Andorre,07,,42.5,1.5166667
```
Lines with missing data are ignored.

** 1d clustering on population size with 10 clusters **
```
yarn jar kmeans.jar /path/to/worldcitiespop.csv /path/to/results.csv 10 4
```

** 2d clustering on city coordinates with 50 clusters **
```
yarn jar kmeans.jar /path/to/worldcitiespop.csv /path/to/path/to/results.csv 50 5 6
```

Arbitrary dimension is supported.
