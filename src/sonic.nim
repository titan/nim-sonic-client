## Sonicclient
## Copyright Ahmed T. Youssef
## nim Sonic client
import asyncdispatch, asyncnet, deques, hashes, json, net, options, os, parseutils, sequtils, strformat, strutils, tables

type
  SonicChannel* {.pure.} = enum
    Ingest
    Search
    Control

type
  SonicBase[TSocket] = ref object of RootObj
    socket: TSocket
    host: string
    port: int
    password: string
    connected: bool
    timeout*: int
    protocol*: int
    bufSize*: int
    channel*: SonicChannel

  Sonic* = ref object of SonicBase[net.Socket]
    events: TableRef[string, proc (data: seq[string])]

  AsyncSonic* = ref object of SonicBase[asyncnet.AsyncSocket]
    currentCommand: Option[string]
    sendQueue: Deque[Future[void]]
    events: TableRef[string, Future[seq[string]]]

type
  SonicServerError = object of Exception

when defined(ssl):
  proc SSLifySonicConnectionNoVerify(Sonic: var Sonic | AsyncSonic) =
    let ctx = newContext(verifyMode=CVerifyNone)
    ctx.wrapSocket(Sonic.socket)

proc quoteText(text: string): string =
  ## Quote text and normalize it in sonic protocol context.
  ##  - text str  text to quote/escape
  ##  Returns:
  ##    str  quoted text

  return '"' & text.replace('"', '\"').replace("\r\n", "") & '"'


proc isError(response: string): bool =
  ## Check if the response is Error or not in sonic context.
  ## Errors start with `ERR`
  ##  - response   response string
  ##  Returns:
  ##    bool  true if response is an error.

  response.startsWith("ERR ")


proc raiseForError(response: string): string =
  ## Raise SonicServerError in case of error response.
  ##  - response message to check if it's error or not.
  ##  Returns:
  ##    str the response message
  if isError(response):
    raise newException(SonicServerError, response)
  return response


proc startSession*(this: Sonic | AsyncSonic): Future[void] {.multisync.} =
  let resp = await this.socket.recvLine()

  if "CONNECTED" in resp:
    this.connected = true

  var channelName = ""
  case this.channel:
    of SonicChannel.Ingest:  channelName = "ingest"
    of SonicChannel.Search:  channelName = "search"
    of SonicChannel.Control: channelName = "control"

  let msg = fmt"START {channelName} {this.password} \r\n"
  await this.socket.send(msg)  #### start
  discard await this.socket.recvLine()  #### started. FIXME extract protocol bufsize

proc open*(host = "localhost", port = 1491, password = "", channel: SonicChannel, ssl = false, timeout = 0): Sonic =
  result = Sonic(
    socket: newSocket(buffered = true),
    host: host,
    port: port,
    password: password,
    channel: channel,
    events: newTable[string, proc (data: seq[string])](),
  )
  result.timeout = timeout
  result.channel = channel
  when defined(ssl):
    if ssl == true:
      SSLifySonicConnectionNoVerify(result)
  result.socket.connect(host, port.Port)

  result.startSession()

proc openAsync*(host = "localhost", port = 1491, password = "", channel:SonicChannel, ssl = false, timeout = 0): Future[AsyncSonic] {.async.} =
  ## Open an asynchronous connection to a Sonic server.
  result = AsyncSonic(
    socket: newAsyncSocket(buffered = true),
    host: host,
    port: port,
    password: password,
    channel: channel,
    events: newTable[string, Future[seq[string]]](),
    sendQueue: initDeque[Future[void]](),
  )
  when defined(ssl):
    if ssl == true:
      SSLifySonicConnectionNoVerify(result)
  result.timeout = timeout
  await result.socket.connect(host, port.Port)
  await result.startSession

proc finaliseCommand(this: Sonic | AsyncSonic) =
  when this is AsyncSonic:
    this.currentCommand = none(string)
    if this.sendQueue.len > 0:
      let fut = this.sendQueue.popFirst()
      fut.complete()

proc recvManaged*(this: Sonic | AsyncSonic): Future[string] {.multisync.} =
  when this is Sonic:
    while true:
      if this.timeout == 0:
        result = this.socket.recvLine
      else:
        result = this.socket.recvLine(timeout=this.timeout)
      if result.startsWith("EVENT "):
        let
          tokens = result.strip.splitWhitespace
          event = tokens[2]
        if this.events.hasKey(event):
          this.events[event](tokens[3..^1])
          this.events.del(event)
      else:
        result = raiseForError(result.strip)
        break
  else:
    while true:
      let resp = await this.socket.recvLine
      if resp.startsWith("EVENT "):
        let
          tokens = resp.strip.splitWhitespace
          event = tokens[2]
        if this.events.hasKey(event):
          this.events[event].complete(tokens[3..^1])
          this.events.del(event)
      else:
        result = raiseForError(resp.strip)
        break

proc sendManaged*(this: Sonic | AsyncSonic, data: string): Future[void] {.multisync.} =
  when this is Sonic:
    this.socket.send(data)
  else:
    proc doSend() =
      this.currentCommand = some(data)
      asyncCheck this.socket.send(data)
    if this.currentCommand.isSome:
      # Queue this send.
      let sendFut = newFuture[void]("sonic.sendManaged")
      this.sendQueue.addLast(sendFut)
      await sendFut
    doSend()

proc execCommand*(this: Sonic | AsyncSonic, command: string, args:seq[string]): Future[void] {.multisync.} =
  let cmdArgs = concat(@[command], args)
  let cmdStr = join(cmdArgs, " ").strip
  await this.sendManaged(cmdStr & "\r\n")

proc execCommand*(this: Sonic | AsyncSonic, command: string): Future[void] {.multisync.} =
  await this.execCommand(command, @[""])

proc ping*(this: Sonic | AsyncSonic): Future[bool] {.multisync.} =
  ## Send ping command to the server
  ## Returns:
  ## bool  True if successfully reaching the server.
  await this.execCommand("PING")
  let resp = await this.recvManaged
  this.finaliseCommand

  result = resp == "PONG"

proc quit*(this: Sonic | AsyncSonic): Future[string] {.multisync.} =
  ## Quit the channel and closes the connection.
  await this.execCommand("QUIT")
  result = await this.recvManaged
  this.finaliseCommand
  this.socket.close

## TODO: check help.
proc help*(this: Sonic | AsyncSonic, arg: string): Future[string] {.multisync.} =
  ## Sends Help query.
  await this.execCommand("HELP", @[arg])
  result = await this.recvManaged
  this.finaliseCommand

proc push*(this: Sonic | AsyncSonic, collection, bucket, objectName, text: string, lang = ""): Future[bool] {.multisync.} =
  ## Push search data in the index
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - objectName: object identifier that refers to an entity in an external database, where the searched object is stored (eg. you use Sonic to index CRM contacts by name; full CRM contact data is stored in a MySQL database; in this case the object identifier in Sonic will be the MySQL primary key for the CRM contact)
  ##   - text: search text to be indexed can be a single word, or a longer text; within maximum length safety limits
  ##   - lang: ISO language code
  ##   Returns:
  ##     bool  True if search data are pushed in the index.
  let
    langString = if lang != "": fmt"LANG({lang})" else: ""
    text = quoteText(text)
  await this.execCommand("PUSH", @[collection, bucket, objectName, text, langString])
  result = (await this.recvManaged()) == "OK"
  this.finaliseCommand

proc pop*(this: Sonic | AsyncSonic, collection, bucket, objectName, text: string): Future[int] {.multisync.} =
  ## Pop search data from the index
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - objectName: object identifier that refers to an entity in an external database, where the searched object is stored (eg. you use Sonic to index CRM contacts by name; full CRM contact data is stored in a MySQL database; in this case the object identifier in Sonic will be the MySQL primary key for the CRM contact)
  ##   - text: search text to be indexed can be a single word, or a longer text; within maximum length safety limits
  ##   Returns:
  ##     int
  let text = quoteText(text)
  await this.execCommand("POP", @[collection, bucket, objectName, text])
  let resp = await this.recvManaged
  result = resp.split[^1].parseInt
  this.finaliseCommand

proc count*(this: Sonic | AsyncSonic, collection, bucket: string, objectName: string = ""): Future[int] {.multisync.} =
  ## Count indexed search data
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - objectName: object identifier that refers to an entity in an external database, where the searched object is stored (eg. you use Sonic to index CRM contacts by name; full CRM contact data is stored in a MySQL database; in this case the object identifier in Sonic will be the MySQL primary key for the CRM contact)
  ## Returns:
  ## int  count of index search data.
  await this.execCommand("COUNT", @[collection, bucket, objectName])
  result = (await this.recvManaged).splitWhitespace()[1].parseInt
  this.finaliseCommand

proc flushCollection*(this: Sonic | AsyncSonic, collection: string): Future[int] {.multisync.} =
  ## Flush all indexed data from a collection
  ##  - collection index collection (ie. what you search in, eg. messages, products, etc.)
  ##   Returns:
  ##     int  number of flushed data
  await this.execCommand("FLUSHC", @[collection])
  result = (await this.recvManaged).splitWhitespace()[1].parseInt
  this.finaliseCommand

proc flushBucket*(this: Sonic | AsyncSonic, collection, bucket: string): Future[int] {.multisync.} =
  ## Flush all indexed data from a bucket in a collection
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   Returns:
  ##    int  number of flushed data
  await this.execCommand("FLUSHB", @[collection, bucket])
  result = (await this.recvManaged).splitWhitespace()[1].parseInt
  this.finaliseCommand

proc flushObject*(this: Sonic | AsyncSonic, collection, bucket, objectName: string): Future[int] {.multisync.} =
  ## Flush all indexed data from an object in a bucket in collection
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - objectName: object identifier that refers to an entity in an external database, where the searched object is stored (eg. you use Sonic to index CRM contacts by name; full CRM contact data is stored in a MySQL database; in this case the object identifier in Sonic will be the MySQL primary key for the CRM contact)
  ##   Returns:
  ##     int  number of flushed data
  await this.execCommand("FLUSHO", @[collection, bucket, objectName])
  result = (await this.recvManaged).splitWhitespace()[1].parseInt
  this.finaliseCommand

proc flush*(this: Sonic | AsyncSonic, collection: string, bucket="", objectName=""): Future[int] {.multisync.} =
  ## Flush indexed data in a collection, bucket, or in an object.
  ##   - collection: index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket: index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - objectName: object identifier that refers to an entity in an external database, where the searched object is stored (eg. you use Sonic to index CRM contacts by name; full CRM contact data is stored in a MySQL database; in this case the object identifier in Sonic will be the MySQL primary key for the CRM contact)
  ##   Returns:
  ##     int  number of flushed data
  if bucket == "" and objectName == "":
    result = await this.flushCollection(collection)
  elif bucket != "" and objectName == "":
    result = await this.flushBucket(collection, bucket)
  elif objectName != "" and bucket != "":
    result = await this.flushObject(collection, bucket, objectName)

proc query*(this: AsyncSonic, collection, bucket, terms: string, limit = 10, offset: int = 0, lang = ""): Future[seq[string]] {.async.} =
  ## Query the database
  ##  - collection index collection (ie. what you search in, eg. messages, products, etc.)
  ##  - bucket index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##  - terms text for search terms
  ##  - limit a positive integer number; set within allowed maximum & minimum limits
  ##  - offset a positive integer number; set within allowed maximum & minimum limits
  ##  - lang an ISO 639-3 locale code eg. eng for English (if set, the locale must be a valid ISO 639-3 code; if not set, the locale will be guessed from text).
  ##  Returns:
  ##    list  list of objects ids.
  let
    limitString = fmt"LIMIT({limit})"
    langString = if lang != "": fmt"LANG({lang})" else: ""
    offsetString = fmt"OFFSET({offset})"
    termsString = quoteText(terms)
  await this.execCommand("QUERY", @[collection, bucket, termsString, limitString, offsetString, langString])
  let
    event = (await this.recvManaged()).splitWhitespace()[^1]
    eventFut = newFuture[seq[string]]("sonic.query")

  this.events[event] = eventFut
  this.finaliseCommand

  result = await eventFut


proc query*(this: Sonic, collection, bucket, terms: string, limit = 10, offset: int = 0, lang = "", callback: proc (data: seq[string]): void): void =
  ## Query the database
  ##  - collection index collection (ie. what you search in, eg. messages, products, etc.)
  ##  - bucket index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##  - terms text for search terms
  ##  - limit a positive integer number; set within allowed maximum & minimum limits
  ##  - offset a positive integer number; set within allowed maximum & minimum limits
  ##  - lang an ISO 639-3 locale code eg. eng for English (if set, the locale must be a valid ISO 639-3 code; if not set, the locale will be guessed from text).
  ##  Returns:
  ##    list  list of objects ids.
  let
    limitString = fmt"LIMIT({limit})"
    langString = if lang != "": fmt"LANG({lang})" else: ""
    offsetString = fmt"OFFSET({offset})"
    termsString = quoteText(terms)
  this.execCommand("QUERY", @[collection, bucket, termsString, limitString, offsetString, langString])
  let event = this.recvManaged.splitWhitespace[^1]

  this.events[event] = callback

proc suggest*(this: AsyncSonic, collection, bucket, word: string, limit = 10): Future[seq[string]] {.async.} =
  ## auto-completes word.
  ##   - collection index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - word word to autocomplete
  ##   - limit a positive integer number; set within allowed maximum & minimum limits (procault: {None})
  ##   Returns:
  ##     list list of suggested words.
  let
    limitString = fmt"LIMIT({limit})"
    wordString = quoteText(word)
  await this.execCommand("SUGGEST", @[collection, bucket, wordString, limitString])

  let
    event = (await this.recvManaged()).splitWhitespace()[^1]
    eventFut = newFuture[seq[string]]("sonic.suggest")

  this.events[event] = eventFut
  this.finaliseCommand

  result = await eventFut

proc suggest*(this: Sonic, collection, bucket, word: string, limit = 10, callback: proc (data: seq[string])): void =
  ## auto-completes word.
  ##   - collection index collection (ie. what you search in, eg. messages, products, etc.)
  ##   - bucket index bucket name (ie. user-specific search classifier in the collection if you have any eg. user-1, user-2, .., otherwise use a common bucket name eg. generic, procault, common, ..)
  ##   - word word to autocomplete
  ##   - limit a positive integer number; set within allowed maximum & minimum limits (procault: {None})
  ##   Returns:
  ##     list list of suggested words.
  let
    limitString = fmt"LIMIT({limit})"
    wordString = quoteText(word)
  this.execCommand("SUGGEST", @[collection, bucket, wordString, limitString])
  let event = this.recvManaged.splitWhitespace[^1]

  this.events[event] = callback

proc trigger*(this: Sonic | AsyncSonic, action = ""): Future[string] {.multisync.} =
  ## Trigger an action
  ##   action text for action
  await this.execCommand("TRIGGER", @[action])
  result = await this.recvManaged()
  this.finaliseCommand
