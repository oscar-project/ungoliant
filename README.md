# Ungoliant

<img align="left" src="img/logo.png" width="200" height="200" /> 

![](https://img.shields.io/crates/d/ungoliant?style=flat-square) ![](https://img.shields.io/crates/l/ungoliant?style=flat-square) 
[![codecov](https://codecov.io/gh/oscar-corpus/ungoliant/branch/master/graph/badge.svg?token=Q3M8F86E2G)](https://codecov.io/gh/oscar-corpus/ungoliant)

🕷️ **Ungoliant is a high-performance pipeline that provides tools to build corpus generation pipelines from CommonCrawl.** 🕷️

It currently is the generation pipeline for [OSCAR corpus](https://oscar-corpus.com), from [CommonCrawl](https://commoncrawl.org).
Ungoliant is a replacement of [goclassy](https://github.com/oscar-corpus/goclassy).


![](https://img.shields.io/github/workflow/status/oscar-corpus/ungoliant/Rust/master?label=main&style=flat-square)                           ![](https://img.shields.io/github/workflow/status/oscar-corpus/ungoliant/Rust/dev?label=dev&style=flat-square)

## Installation

### Installing/Compiling the binary
* Via `cargo`: `cargo install ungoliant`
* Via `git`: `cargo install --git https://github.com/oscar-corpus/ungoliant`

Ungoliant needs numerous dependencies that should be compiled when installing. However `cmake / gcc` can be needed as the project uses [fasttext-rs](https://github.com/messense/fasttext-rs).

### KenLM feature

The KenLM feature is optional because it relies on unsafe code that can break if the supplied model files are not correct.

To enable it, install KenLM requirements:

```bash
apt install -y libboost-all-dev libeigen3-dev
```

and use `cargo install ungoliant --feature kenlm` or `cargo b --features kenlm` if you're building from source.

### Getting the language identification file (for fastText):

Use `curl https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -o lid.176.bin`.

## Usage 

The usual way of generating corpora is:

1. Fetch the `wet.paths.gz` file from the last [CommonCrawl dump](https://commoncrawl.org/connect/blog/) and decompress it.
2. Download the files using the `download` command.
3. Generate the corpus using the `pipeline` command (it may take some time).
4. Head on to [oscar-tools](https://github.com/oscar-project/oscar-tools) for the packaging steps

You can find more information on each command's `--help`.

```text
ungoliant 2
corpus generation tool.

USAGE:
    ungoliant <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    download    Download a CommonCrawl release
    help        Prints this message or the help of the given subcommand(s)
    pipeline    Run pipeline
    rebuild     Rebuild the corpus for a given language.
```

## Documentation

Ungoliant is not yet on docs.rs: use `cargo doc --bins --open` to open the documentation.

Head on to [OSCAR Documentation](https://oscar-project.github.io/documentation/) for more info about the project.

