
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrowextendr

<!-- badges: start -->
<!-- badges: end -->

The goal of arrowextendr is to …

## Installation

You can install the development version of arrowextendr from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("JosiahParry/arrow-extendr")
```

These R functions illustrate that we can create arrow-rs structs and
return them to R

``` r
library(arrowextendr)

# i32 array
test_i32()
#> Array
#> <int32>
#> [
#>   1,
#>   null,
#>   3
#> ]

# f64 array
test_f64()
#> Array
#> <double>
#> [
#>   1,
#>   null,
#>   3
#> ]

# fields
test_field()
#> Field
#> field_name: binary

# record batches
test_record_batch()
#> RecordBatch
#> 5 rows x 1 columns
#> $id <int32 not null>
```
