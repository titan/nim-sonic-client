import asyncdispatch
import sonic
import unittest

suite "Ingest Sync Test":

  setup:
    let client: Sonic = open(channel = SonicChannel.Ingest)

  teardown:
    discard client.quit

  test "PING":
    let resp = client.ping

    check:
      resp == true

  test "PUSH":
    check true == client.push("wiki", "articles", "article-1", "for the love of god hell")
    check true == client.push("wiki", "articles", "article-2", "for the love of satan heaven")
    check true == client.push("wiki", "articles", "article-3", "for the love of lorde hello")
    check true == client.push("wiki", "articles", "article-4", "for the god of loaf helmet")

  test "POP":
    check 2 == client.pop("wiki", "articles", "article-1", "for the love of god hell")
    check 0 == client.pop("wikis", "articles", "article-1", "for the love of god hell")

  test "COUNT":
    check 0 == client.count("wiki", "articles", "article-1")
    check 3 == client.count("wiki", "articles", "article-2")
    check 0 == client.count("wiki", "articles")

  test "FLUSHO":
    check 0 == client.flushObject("wiki", "articles", "article-1")
    check 3 == client.flushObject("wiki", "articles", "article-2")

  test "FLUSHB":
    check 1 == client.flushBucket("wiki", "articles")

  test "FLUSHC":
    check 1 == client.flushCollection("wiki")

suite "Search Sync Test":
  let client: Sonic = open(channel = SonicChannel.Search)
  let ingest: Sonic = open(channel = SonicChannel.Ingest)

  setup:
    discard ingest.push("wiki", "articles", "article-1", "for the love of god hell")
    discard ingest.push("wiki", "articles", "article-2", "for the love of satan heaven")
    discard ingest.push("wiki", "articles", "article-3", "for the love of lorde hello")
    discard ingest.push("wiki", "articles", "article-4", "for the god of loaf helmet")

  teardown:
    discard ingest.flushCollection("wiki")

  test "QUERY":
    proc successCallback(data: seq[string]) =
      check:
        len(data) == 3
        "article-3" == data[0] and "article-2" == data[1] and "article-1" == data[2]
    client.query("wiki", "articles", "love", callback = successCallback)
    discard client.ping

    proc failureCallback(data: seq[string]) =
      check len(data) == 0
    client.query("wiki", "articles", "hate", callback = failureCallback)
    discard client.ping

  test "SUGGEST":
    proc callback(data: seq[string]) =
      check len(data) == 0
    client.suggest("wiki", "articles", "lo", callback = callback)
    discard client.ping

suite "Ingest Async Test":

  setup:
    let client: AsyncSonic = waitFor openAsync(channel = SonicChannel.Ingest)

  teardown:
    discard waitFor client.quit

  test "PING":
    let resp = waitFor client.ping

    check:
      resp == true

  test "PUSH":
    check true == waitFor client.push("wiki", "articles", "article-1", "for the love of god hell")
    check true == waitFor client.push("wiki", "articles", "article-2", "for the love of satan heaven")
    check true == waitFor client.push("wiki", "articles", "article-3", "for the love of lorde hello")
    check true == waitFor client.push("wiki", "articles", "article-4", "for the god of loaf helmet")

  test "POP":
    check 2 == waitFor client.pop("wiki", "articles", "article-1", "for the love of god hell")
    check 0 == waitFor client.pop("wikis", "articles", "article-1", "for the love of god hell")

  test "COUNT":
    check 0 == waitFor client.count("wiki", "articles", "article-1")
    check 3 == waitFor client.count("wiki", "articles", "article-2")
    check 0 == waitFor client.count("wiki", "articles")

  test "FLUSHO":
    check 0 == waitFor client.flushObject("wiki", "articles", "article-1")
    check 3 == waitFor client.flushObject("wiki", "articles", "article-2")

  test "FLUSHB":
    check 1 == waitFor client.flushBucket("wiki", "articles")

  test "FLUSHC":
    check 1 == waitFor client.flushCollection("wiki")

suite "Search Async Test":

  setup:
    let client: AsyncSonic = waitFor openAsync(channel = SonicChannel.Search)
    let ingest: AsyncSonic = waitFor openAsync(channel = SonicChannel.Ingest)
    discard waitFor ingest.push("wiki", "articles", "article-1", "for the love of god hell")
    discard waitFor ingest.push("wiki", "articles", "article-2", "for the love of satan heaven")
    discard waitFor ingest.push("wiki", "articles", "article-3", "for the love of lorde hello")
    discard waitFor ingest.push("wiki", "articles", "article-4", "for the god of loaf helmet")

  teardown:
    discard waitFor ingest.flushCollection("wiki")
    discard waitFor ingest.quit
    discard waitFor client.quit

  test "QUERY":
    let respfut = client.query("wiki", "articles", "love")
    discard waitFor client.ping
    if respfut.finished:
      let data = respfut.read
      check:
        len(data) == 3
        "article-3" == data[0] and "article-2" == data[1] and "article-1" == data[2]
    else:
      fail()

    let emptyrespfut = client.query("wiki", "articles", "hate")
    discard waitFor client.ping
    if emptyrespfut.finished:
      let data = emptyrespfut.read
      check:
        len(data) == 0
    else:
      fail()

  test "SUGGEST":
    let respfut = client.suggest("wiki", "articles", "lo")
    discard waitFor client.ping
    if respfut.finished:
      let data = respfut.read
      check len(data) == 0
    else:
      fail()
