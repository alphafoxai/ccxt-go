# ccxt Go v4 mirror for AlphaFox

This repository holds the `go/v4` module of [ccxt](https://github.com/ccxt/ccxt) (MIT),
taken from `github.com/ccxt/ccxt/go/v4` at release **v4.5.77**, with one change:
`gate.go` signs and sends the same query key order.

Why a separate repository: resolving that module from `ccxt/ccxt` (or from a fork of
it) makes the Go toolchain clone every ref of the source repository — about 1.06M refs
and 8.4 GB — which does not fit in a CI job. This repository carries only the module
source (about 35 MB, one commit), so the fetch takes seconds.

The module sits at the repository root and keeps the upstream declared path
`github.com/ccxt/ccxt/go/v4`, because 106 files inside the module import that path.
The repository path carries the `/v4` version suffix, so Go resolves the tag `v4.5.77`.
The module is only usable through a `replace`, which supplies the import paths used by
the consumer.

Consumers pin it with:

    replace github.com/ccxt/ccxt/go/v4 => github.com/alphafoxai/ccxt-go/v4 v4.5.77

The upstream report for the Gate fix is https://github.com/ccxt/ccxt/issues/30561 and
the proposed upstream patch is https://github.com/ccxt/ccxt/pull/30562. Delete this
repository and the `replace` directives once the fix ships upstream.
