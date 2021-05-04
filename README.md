# Ungoliant

:spider: Ungoliant is the upcoming pipeline to obtain an [OSCAR corpus](https://oscar-corpus.com) from a [Common Crawl](https://commoncrawl.org) dump. This pipeline replaces the original [goclassy](https://github.com/oscar-corpus/goclassy) pipeline.

## Compilation

If your server does not have OpenSSL installed and you don't have administrator privileges, install [anacoda](https://www.anaconda.com/products/individual), [install OpenSSL with anaconda](https://anaconda.org/anaconda/openssl) and then export these variables:

```bash
export OPENSSL_DIR=/path/to/anaconda/bin
export OPENSSL_LIB_DIR=/path/to/anaconda/lib
export OPENSSL_INCLUDE_DIR=/path/to/anaconda/include
export OPENSSL_STATIC=1
```

## Benchmarking

Use `cargo bench` to run benchmarking.

See results in `target/criterion/report/index.html`