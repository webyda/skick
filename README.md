# Skick
Skick is a library for building actor based back ends for web applications. It
allows clients to connect over a websocket and interact with *actors*
using JSON encoded messages.

A simple hello world program would look something like:

```python
from skick import Skick

skick = Skick()

@skick.session("hello")
def hello_world(inst, message):
    """ A hello world type user session """
    @inst.on_start
    async def on_start():
        await inst.socksend({"greeting": "Hello, use the action 'greet' to receive a greeting"})
    
    @inst.socket("greet", {"action": "greet"})
    async def greet(msg):
        await inst.socksend({"greeting": "Hello good sir"})

skick.start()
```

In the example above, a single core instance of Skick is instantiated and a websocket server is opened on
port 8000. A so called "session" is declared and registered with the system.
This allows users to connect over a websocket and request a "hello" session
using the command `{"session_type": "hello"}`. If they do, a pair of actors,
a *receptionist* and a *session actor* will be instantiated on the server and
associated with the connection. The receptionist receives and sends messages
over the websocket and filters them to ensure they conform to predefined
schemas. The session actor receives the accepted messages and processes them
within the actor model.

In this particular case, the session actor will begin by sending the message
```
{"greeting": "Hello, use the action 'greet' to receive a greeting"}
```
to the client, and will then proceed to await further communications from the
client. This particular session type has only one registered schema, namely
the one associated with the "greet" action. If the client sends a conforming
message, the method greet will be scheduled and run resulting in the following
interaction

```
Client: {"session_type": "hello"}
Server: {"greeting": "Hello, use the action 'greet' to receive a greeting"}
Client: {"action": "greet"}
Server: {"greeting": "Hello good sir"}
```

This demonstrates some of the fundamental ideas behind Skick. Actors should
be defined in much the same way as endpoints in the most popular HTTP
frameworks like Flask and Sanic. There should be minimal boilerplate
requirements, and the library should interface seamlessly with a JavaScript
client in a browser. In order to see the full picture, we will need to
take a deeper dive into the features and general architecture of the system.
## The Current State of the Project
Skick is in an experimental state. It is essentially a proof of concept. It has
not been battle tested on the internet, and has not been extensively analyzed
for vulnerabilities. There are certain weak points.

## The Actor
At the heart of the Skick system lies the lone actor. Actors are program units
which have a mailbox attached. They receive messages in this mailbox and
in response to these messages, they can do any combination of the following:

1. Send messages to other actors.
2. Spawn new actors.
3. Replace their current set of responses with a different set of responses.

While this leads to a nice and tidy theoretical model, such as the one
exhibited in Gul Agha's *Actors, A Model of Concurrent Computation in
Distributed Systems* from 1986, it does so at the expense of certain practical
realities of software development. For this reason, additional capabilities have
been added to Skick. In particular, Skick actors also have abilities like:

4. Maintaining state.
5. Running daemons.
6. Running functions on startup and on stopping.
7. Maintaining stateful conversations with other actors.
8. Receiving notices when actors cease to run.

### Defining Actors
Actors in Skick are defined in *synchronous* declarative functions which are
registered by a special *shard* actor (conveniently hidden away in the Skick
object). The actor is instantiated by this shard actor when it is needed. The
creation process proceeds in the following steps:

1. The shard actor creates an Actor object.
2. The shard actor runs the actor's declarative function on the actor object.
3. The function stores the actor's state in the closures of the functions defined within, and registers message
   handlers (behaviors in actor terminology), daemons and other necessary
   methods.
4. The actor runs the on_start method if one has been registered.
5. The actor starts separate tasks for the daemons (be careful of race
   conditions).
6. The actor starts the message processing loop in the main task.

In practice, defining actors is done through the `actor` method in some Skick instance
```python
@skick.actor(actor_name)
def function_name(actor_instance, init_message):
    ...
```
Here, `actor_name` is a string used to refer to the actor type when requesting
it from the shard. The `actor_instance` is the instance of the Actor class that
the shard has prepared for us, and the `init_message` is some *message*
(messages in skick are always dictionaries) that helps us set up the initial
state of the actor.

Within the function, the `actor_instance` contains methods which decorate
asynchronous functions in various ways. These are

1. `@inst.action(action_name, [optional schema])` which defines a response to
   a particular message type
2. `@inst.on_start` which registers an asynchronous method to run on startup.
   N.B: It is very important to run time consuming initiation steps such as
   fetching information from an API asynchronously in this `on_start` function.
   If you do not do this, then you will hold up the shard actor,
   freezing the entire shard.
3. `@inst.on_stop` which acts as the inverse of `on_start`, being run during
   the ordinary shutdown procedure.
4. `@inst.daemon(name)` which registers an asynchronous function that will run in
   a separate task under the name `name`.
5. `@inst.conversation(name)` which will be discussed later, but which formerly
   decorated asynchronous generator functions to produce the stateful
   conversation mechanism. More on this later.

To illustrate how these parts function together, an example is in order.

```python
@skick.actor("accumulator")
def accumulator(inst, message):
    """ An actor that adds numbers to an internal tally. """
    tally = message.get("initial_tally", 0)

    @inst.daemon("tally_reporter")
    async def report():
        """ Reports the current tally with regular intervals if asked to """
        while "report_to" in message:
            await asyncio.sleep(1)
            await inst.send(message["report_to"],
                            {"action": "report_tally", "tally": tally})

    @inst.action("add", schema={"action": "add", "number": int})
    async def adder(msg):
        """ Adds msg["number"] to the tally found in the closure """
        nonlocal tally # Necessary nonlocal declaration for immutable objects
        tally += msg["number"]
    
    @inst.action("get_tally", schema={"action": "get_tally", "sender": str})
    async def get_tally(msg):
        """
        Asks the actor to send the current tally to *sender*. This could
        have used the conversation mechanism which we will introduce later.
        """
        await inst.send(msg["sender"], {"action": "report_tally", "tally": tally})
```
### Helper Methods
There are a number of helpful methods in the actor instance.

1. `async inst.send(address, message)` which sends the message (a dictionary) to
   to the actor that has the address *address*. In general, this address must be
   known in advance.
2. `async inst.replace(new_type, message)` which replaces the current actor's
   behaviors with a new set of behaviors defined in some actor definition.
   This function strips the actor of all but its address, purging
   all daemons, all message handlers, conversations, all its closures etc.
3. `async inst.converse(partner, initial_message, prorotype)` which initiates a
   conversation with the `partner` actor. More on this later.
4. `async inst.spawn(actor_type, message, ...)` which asks the shard object to
   spawn a new actor of the type `actor_type` and send it the initial message
   `message`. By default, this is spawned on the same shard in clusters, and
   the requesting actor is added as a monitor so that it will be notified when
   the new actor dies. 
5. `register_service(name)` which registers the actor in the service registry
   as probiding the service `name`
6. `unregister_service()` which removes all mentions of the actor from the service
   registry
7. `get_service(name, local="mixed")`, which retrieves a random service provider
   for the service, either from
   1. `local="local"` only shard local providers;
   2. `local="mixed"` local or remote shards, preferring local shards if available; or
   3. `local="remote"` only remote providers.

### How to Run Actors
Actors that have been defined in the manner described above are not
automatically instantiated. They have to be requested somewhere in the program.
Apart from being spawned as a response to user connections in the session system,
shards may be spawned by any actor using the `spawn` method. In order to spawn
actors when the shard starts up, the user can define an `on_start` method. 

This method is scheduled to run when the shard starts up and is supplied with a
direct reference to the shard actor, so that we may use it to spawn more actors
as shown in the example below:

```python
from skick import Skick

async def on_start(shard):
    await shard.spawn("accumulator", {"initial_tally": 420})

skick = Skick(on_start=on_start)

...

skick.start()
```

### Conversations
Often, actors have to communicate back and forth in specific sequences. This may
be as simple as actor A asking actor B what the value of some variable is.
With the basic messaging functionality we have to solve this problem through a
combination of complicated state management, replacement and defining receiving
and sending actions in separate methods.

This is *too* inconvenient in many situations. For this reason, Skick provides
an alternative out of the box called a *conversation*. A conversation takes
place between two actors, one caller and one responder. These take turns in passing
messages to eachother in a call and response fashion.

In a conversation in Skick, the actions involved are not normal asynchronous
functions, but asynchronous generator functions. Messages are sent and received 
through the use of the `yield` keyword. Conversations are set up by yielding
special `Conversation` objects, subclassed in one `Call` and one `Respond` variety
for convenience, and are managed by the actor objects out of the view
of the programmer. Suppose the Accumulator actor from before were to return the
new tally whenever it received an "add" request. We could rewrite the action as
a conversation in the following manner.

```python

...

from skick import Skick, Respond

...

@skick.actor("accumulator")
def accumulator(inst, message):
    """ An actor that adds numbers to an internal tally. """
    tally = message.get("initial_tally", 0)

    ...

    @inst.action("add")
    async def adder(msg):
        """
        Here, because adder is responding in a conversation, the msg parameter
        is generated by the calling actor, and is not manually constructed by
        the programmer. It contains the necessary parameters to respond to the
        call.
        """
        nonlocal tally # Necessary nonlocal declaration for immutable objects
        yield Respond(msg) # We first "pick up the phone" as it were
        
        tally += msg["number"]
        yield {"tally": tally} # Then we yield a response
```
In the corresponding caller, another asynchronous generator is found:

```python
...

from skick import Skick, Call

...

@skick.actor("caller")
def caller(inst, message):

    ...

    @inst.action("call_adder")
    async def call_adder(msg):
        new_tally = yield Call(adder_address, {"number": 10})

    ...
```
These conversations can go on for many exchanges, each (with the exception of the `yield Respond(...)`
call) taking the form:
```python
    ...
    response_to_our_message = yield our_message
    ...
```

In the caller actor above, the conversation was initiated because we received
a message the handler of which was a *conversation prototype*. This is not always
appropriate. For example, we may wish to start conversations in the `on_start` method
or in some daemon. If we wish to do this, we can create a conversation using the
`inst.converse` method. `inst.converse` can be called in two ways. Either,
it can be used to invoke a registered prototype by passing its string identifier,
or we can pass some asynchronous generator function. In other words, we could
imagine a scenario like the following.

```python
@skick.actor("caller")
def caller(inst, message):

    ...

    @inst.on_start
    async def init():

        ...

        async def send_number(msg):
            response = yield Call(adder_address, {"number": 720})
            await do_something(response)

        await inst.converse({}, send_number)

    ...
```

## The Special Actors
There are two special actors in Skick. The *shard* actor and the *WebSocket*
actor. Both are automatically instantiated by the Skick object, but the latter
can be disabled if you do not wish to enable websocket functionality.

These actors help the other actors in various ways.

### The Shard Actor
Skick instances may run in clusters, but even if they are run in a single core,
there is a special *shard* actor in the background. This actor performs a few
helpful jobs for the actors.

1. It maintains a record of which actors are alive so they don't get garbage
   collected.
2. It maintains a registry of *services* which works like an internal DNS to which
   actors can add themselves if they provide a service to other actors and from
   which actors can request addresses to service providers.
3. If there are several shards in the cluster, it will communicate with the other
   shards to ensure it has up to date information about which services and actor types are available
   on the other shards, as well as telling the other shards which services and actor types it provides.
4. It helps spawn actors

Most of these functions are not exposed to the user through the actual shard actor object.
Instead, the shard injects helpers into all actors so that they can be accessed through any Actor instance.
Actors can also be spawned by sending a message to the shard, but this is
primarily meant for spawning shards remotely.

### The WebsocketActor
If a websocket server has been requested, a special `WebsocketActor` will be instantiated. It maintains its own
collection of websocket handlers called *sessions* and *subsessions* as well as so called *handshake sequences*.
These are abstractions of lower level actors, to which websocket connections have been affixed.
The exact details are complicated enough to warrant their own chapter, but these special actors live in their own
universe, which is managed by the `WebsocketActor`. The `WebsocketActor` also ensures
that the actors associated with the connection live and die together, and that they are both stopped when the connection terminates.

## The Session and the Subsession
In the previous sections, we discussed features that live, so to speak,
inside the clean room that your backend should be. However, for our backend to
be useful for anything, it has to be able to communicate with the outside world.
Most importantly, it has to be able to communicate with clients over the internet.
Skick's main method for this is to use *websockets*. Websockets allow bidirectional
communication over persistent TCP connections with clients like web browsers
and phone apps.

The way Skick handles these connections is to create a pair of actors for each incoming connection.
This syzygy of actors is called a *subsession*. If the client is able to directly request
the subsession type over the websocket, then it is called a *session*. All the other subsession types
are created through subsession replacement.

Let us recall the Hello World program from earlier:

```python

...

@skick.session("hello")
def hello_world(inst, message):
    """ A hello world type user session """
    @inst.on_start
    async def on_start():
        await inst.socksend({"greeting": "Hello, use the action 'greet' to receive a greeting"})
    
    @inst.socket("greet", {"action": "greet"})
    async def greet(msg):
        await inst.socksend({"greeting": "Hello good sir"})

...
```

We note that the `skick.session` function is very similar to an actor declaration.
It uses similar methods, like `@inst.on_start`. On top of this there are new methods,
such as `@inst.socket` and `inst.socksend`. These serve to give us the illusion
that the session is a special type of actor. In reality, there are two different
subclasses for these special websocket actors, one `FrontActor` and one `BackActor`.
They both provide the same sets of decorators and methods, but the methods and
decorators do not have the same effects on them.

When the `hello_world` function is run on a `FrontActor`, the `@inst.on_start`
decorator simply does nothing (there is a separate `@inst.front_on_start` method
for the exceptional case where you would want to run code on startup in the
`FrontActor`). When the `@inst.socket` decorator is used, the `FrontActor` merely
records the name of the method and the provided schema, but completely ignores
the function body. Likewise, the old `@inst.action` decorator has been
overridden and does nothing on the `FrontActor`.

When the `hello_world` function is run on a `BackActor`, the `@inst.on_start`
and other standard decorators work as we would expect from a normal `Actor`.
They *target* the `BackActor` since it is the actual main actor of the session.
As for the `@inst.socket` decorator, it reverts to an `@inst.action` decorator
ignoring the schema entirely.

Many methods have these dual interpretations. The `inst.socksend` method,
which only exists on these special websocket actors, sends a message to the
websocket client when it is called on the `FrontActor`, but if it is called in
the `BackActor`, it encapsulates the message in an action that asks the `FrontActor` to
send the encapsulated message over the websocket.

### How Websocket Connections are Processed
When a client requests a new websocket connection, the following steps are followed

1. The underlying websocket library (currently only Websockets is available) creates a new task.
   The task awaits an initial handshake message from the client, which must be
   a dictionary and which must contain the field `"session_type"`.
2. If the session type has been registered with the WebsocketActor, then it creates
   two new actors. One using the `FrontActor` class and one using the `BackActor`
   class.
3. The same `session` definition function is run twice. Once on the FrontActor
   and once on the BackActor function.
4. The websocket actor coordinates these two actors by directly injecting references
   to things like the websocket connection and the other actor in the pair into
   the actor objects.
5. The actors are both started in their own separate tasks.

Once this process has completed, the client can send messages to the server.
If it does, they will be processed in the following order:

1. The request arrives over the websocket connection.
2. It is received by a separate task in the `FrontActor`, where it is deserialized
   and compared with locally available schemas.
3. If there is a corresponding schema, and the deserialized message conforms to it,
   the message is sent over the messaging system to the `BackActor`, where
   the schema has a corresponding `action` which treats the message as any other
   that has been sent over the messaging system.

If the `BackActor` chooses to send a message back to the client the following
takes place:

1. The message is encapsulated into a message which is sent to the `FrontActor`
2. The encapsulated message is received in the `FrontActor` and is processed as
   any other action. The specific action, `"socksend"`, merely extracts the message and sends it
   through a method in the `FrontActor`'s self._websocket field.

### Why
This dual actor setup may seem like an unnecessarily complicated procedure. Surely, it must
introduce a plethora of bugs and cause all manner of performance penalties.
There is a reason why we want this type of division. Originally, it was
conceived that we should separate the actors so that they could live on different shards.
This would separate the shards into those that only dealt with the outside world
and those that dealt only with the innards of the actor system. In such a scenario,
we could scale the these systems independently, and we might be able to prevent
excess user requests from halting the entire system. Additionally, this type of clean
separation does not preclude associating *several* connections to the same session.
That is, a user who is connected on both his smartphone and his laptop could
interact with the system through the same `BackActor` even if he maintains
several `FrontActor`s. These features are not
present in the system for the time being, and we may ultimately shift towards a
single actor setup, where one actor performs all tasks required to maintain websocket
connections. Indeed, we may even settle for a solution where these mechanisms
coexist and can be used interchangeably depending on the context.

### Specifics About the FrontActor's Methods
The FrontActor inherits all methods and decorators from the Actor class, but
in order to access them we need to use different names, namely

* The `@inst.front_action` decorator creates an action on the front actor
* The `@inst.front_daemon` decorator creates a daemon on the front actor
* The `@inst.front_on_start` decorator registers a startup method on the front actor
* The `@inst.front_on_stop` decorator registers a shutdown method on the front actor
* The `async inst.socksend` method sends a message to the websocket client

Apart from these special methods, a number of ordinary methods are either irrelevant
or have had their functionalities altered

1. The `@inst.action` decorator ignores the function it was fed.
2. The `@inst.daemon` decorator ignores the function it was fed.
3. The `@inst.on_start` decorator ignores the function it was fed.
4. The `@inst.on_stop` decorator ignores the function it was fed.
5. The `async inst.replace` method works as usual, but performs additional steps to accommodate for the websocket.
   It is not invoked directly by the user, but is invoked in response to a message sent by the `BackActor`
6. The conversation mechanisms function as usual, but are largely irrelevant.
7. The service registry functions are not disabled, but only `get_service` should be used in practice.

### Specifics About the BackActor's Methods
Like in the case of the `FrontActor`, the `BackActor` inherits all decorators and
methods from the `Actor` class. Some of them function as usual, and some have
had their behaviors altered. In addition to this, all the methods defined on the 
`FrontActor` also exist on the `BackActor` but many of them do nothing. 

1. The `@inst.front_...` decorators all do nothing on the `BackActor`.
2. The `@inst.daemon` and `@inst.action` decorators functions as they do on the
   `Actor` class.
3. The `async inst.socksend` method sends a message to the `FrontActor` instructing it to send a message to the client.
4. The `async inst.replace` method replaces the `BackActor`, but also performs the additional step
   of ordering the `FrontActor` to perform a corresponding replacement action on its end.
   N.B: This operates over the messaging system, and may therefore be somewhat brittle in some situations.
5. The conversation mechanisms function as usual, with the exception of the addition of a
   special method to query the client for input. More on this later.
6. The service registry functions are not disabled but only `get_service` should be used in practice.

### Session Conversations
When conversing with a handler defined through an `@inst.socket` decorator,
the conversation will be with the `BackActor`. The `BackActor` has the ability
to use its turn to interject a user query into the conversation. This is done
by yielding a `skick.SocketQuery` object. When this is done, the `BackActor` will
set up a query in the `FrontActor`. They work by siphoning off client requests where the action field is `"query_reply"`, and comparing the message to registered queries. For this command there is a two tiered schema verification procedure:

1. The message as a whole is evaluated against the schema
   `{"action": "query_reply", "query_id":str, "message": object}`.
2. It is verified that such a query has been registered.
2. The "message" field in the dictionary is then evaluated against the
   *subschema* that was specified by the `BackActor` when the query was made.

The conversation in the `BackActor` will resume once the websocket client has sent its response.

Because the `FrontActor` is unable to read function bodies, the subschemas
can not be specified within the handler requesting the conversation.
Instead, subschemas must created in the session declaration's direct scope
using the function `inst.query_schema`. This is highly inconvenient, so
a number of simple schemas have been predefined for the programmers's convenience, namely:

1. `"default"` which accepts an `int`, `float`, `str` or `bool`
2. `"int"` which accepts an `int`,
3. `"float"` which accepts a `float`,
4. `"str"` which accepts a `str`,
5. `"bool"` which accepts a `bool`,
6. `"list"` which accepts a list consisting of only those types included in the `"default"` subschema.

For this reason, many simple queries require no extra `query_schema` declarations.

In principle, this means we can see constructs like the one below:

```python

...

@skick.session("curious")
def curiosity(inst, message):
   
   ...

   @inst.socket("get_information")
   async def info(msg):
      response = yield Call(info_actor, {"action": "info", "credentials": cred})
      
      if response["ask_gdpr"]:
         consent = yield SocketQuery({"gdpr_query": GDPR_STRING}, "bool")
      
         if consent:
            await inst.socksend(response["info"])
         else:
            await inst.socksend({"error": "terms not accepted"})
      else:
         await inst.socksend(response["info"])
   
   ...

...

```
In the example above, the `BackActor` asks a different actor for some piece of
information. The other actor discovers that the user with the supplied credentials
has not accepted certain important GDPR related terms and conditions. It informs
the `BackActor`, which sends a query to the client. If the client responds approvingly, the information is sent to the client, otherwise an error message
is sent.

### Handshake Sequences
Imagine for a moment that your Skick application has several classes of user sessions. Maybe they represent different user tiers or perhaps one skick instance
runs several different services. In many such situations there are substantial benefits to be gained from allowing us to reuse sequences of sessions and subsessions that the system proceeds through before a fully authenticated and initiated user session is in place. Skick has a mechanism for this called a
*handshake sequence*.

We may declare that a function is a `handshake` by using the `@skick.handshake`
decorator. We begin with an example:
```python
...

@skick.handshake("random_number")
def random_session(inst, message)
   num = random()
   @inst.on_start
   async def on_start():
      await inst.replace("double_number", {**message, "number": num})

@skick.subsession("double_number")
def doubler(inst, message):
   num = 2*message["number"]
   @inst.on_start()
   async def on_start():
      await inst.replace(f"{message['session_type']}:final", {"number": num})

@random_session("logarithm")
def logarithm(inst, message):
   base = message["number"]
   @inst.socket("log", {"action": "log", "x": float})
   async def log(msg):
      await inst.socksend({"log_base(x)": math.log(msg[x], base=base),
                           "x": msg["x"]})
```
Here, the `random_number` and `double_number` subsessions are part of a handshake sequence. `@skick.handshake` does not actually create a session. Instead, it
replaces the function it decorates with a new decorator. This new decorator can be used to create subsessions which are preceded by some standardized succession of
subsessions.

Let us take a closer look at the last decorated function.
```python
@random_session("logarithm")
def logarithm(inst, message):
   ...
```
Here, one session and one subsession are created and registered. One called
`logarithm` and the other called `logarithm:final`. The former is merely a copy of
`random_session`, and the latter is the one actually described in `logarithm`. Skick expects
a subsession that is part of a handshake sequence to forward the `"session_type"`
field of the original message throughout the sequence. Ultimately, the final actor
in the sequence should replace itself with the `f"{message['session_type']}:final"`
actor, handing control over to the subsession in question.

### Session termination
Since sessions are not normal actors, they are terminated in a slightly different fashion than normal actors. If you invoke the `inst.stop` method on the `BackActor`,
the `WebsocketActor` will make sure to close the connection and stop the `FrontActor`.

This works because the `FrontActor`, `BackActor` and the consumer task in the `FrontActor` are collated in a special `Syzygy` object by the `WebsocketActor`.
If any of the three fail, the `Syzygy` provides methods for also shutting down the other two.

A word of caution is appropriate when it comes to these syzygies. While the
consumer task is directly monitored by the `WebsocketActor`, and while the stop
methods are invoked directly without involving messaging, the `WebsocketActor` is informed of the demise of the actors in the `Syzygy` through the sentinel message it
*should* emit. This may prove unreliable on some occasions, and we *might* see situations where only one of the actors is actually alive. We may also experience ill defined states when the sentinel messages are in transition.

## Clustering
Skick has the ability to run on many shards in a cluster. If this is required in your application, then you must provide some supported messaging system in your backend, since Skick does not supply its own distributed messaging system.

At present, Skick relies on RabbitMQ for its inter-shard communication. If you have
an unencrypted RabbitMQ service hidden behind a firewall, you may swap
Skick's default messaging system (which relies on a dictionary of active mail queues)
for a RabbitMQ based system by adding a parameter when instantiating your Skick class.

```python

...

skick = Skick(on_start = on_start,
              message_system = "amqp://user:pwd@rabbit_url:rabbitport")

...

```
If you choose to run skick in a clustered configuration, there is no requirement that the shards be homogeneous. You may opt to have some that do not support websockets, some that use different websocket drivers, some that provide interactions with some external service local to its docker container or whichever
architecture you think is best for your application. No matter how you organize
the system, it will work as long as the shards all use the same messaging system.

## The Skick Object
In the examples above, we interact with the library through an imported class
called Skick. The skick class is responsible for setting up the special actors,
launching all needed tasks and connections, as well as coordinating all the disparate components for the user.

### Options
The Skick object natively supports the following options:

1. `on_start` which allows you to specify a startup function. This would typically be used to spawn actors on startup.
2. `message_system` which, if you supply it, presumes that you have given it an AMQP url to a RabbitMQ instance.
3. `websocket_host` which, if you supply it, is the address the websocket library will listen on
4. `websocket_port` which, if you supply it, is the port the websocket library will listen on
5. `websocket_server` which, if you set it to any value, will disable the websocket server
6. `ssl` which tells the websocket library to use TLS. It can be
   1. A string, in which case skick will assume it is a PEM file and will use it to generate an SSLContext object for you using python's `create_default_context` method.
   2. A tuple, in which case it will act as above, but it will presume the first element is the PEM file and the second is the password.
   3. An SSLContext instance, in which case it will be used by the websocket server.
7. `websocket_options` which should be a dictionary of options that will be passed directly to the websocket library.
8. `dict_type` which, if you provide a string, will be presumed to be a redis url to a redis server that will manage your `hash` objects.

### Methods
The `Skick` instance also supplies a number of methods. Some of these are
actual methods from the class defintion and some are merely proxies for methods
in the underlying `Shard` object.

1. `skick.start()`, which starts the event loop and the rest of the system.
2. `skick.stop()`, which initiates the shutdown procedure.
3. `skick.add_directory(directory)` which adds the actors, subsessions and handshakes in the directory to the skick instance.
4. `@skick.actor(name)` which directly registers an actor type with the shard.
5. `@skick.subsession(name)` which directly registers a subsession type with the websocket actor.
6. `@skick.session(name)` which directly registers a session type with the websocket actor.
7. `@skick.handshake(name)` which directly registers a handshake with the websocket actor.
8. `skick.hash(name)` which provides a hash table as described below using the requested backend.

## Sentinels
In an actor model, we frequently end up having a large number, perhaps tens of thousands of concurrent actors. In the absence of some tool to keep track of these,
it is very easy to see how actors could be lost and forgotten, leading to memory leaks, or how errors from crashing actors might go unknown, or how any number of other things might go wrong.

Most actor systems use some sort of system for supervising actors for this reason. In Skick, the supervision is carried out by *sentinels*, tasks that monitor other tasks. If they detect a cancellation, exception or a task being marked as done, they will report this to the actor's *monitors*. Monitors are other actors that have told the supervised actor that they wish to be notified.

### Registering as a Monitor
When an actor is spawned, we may supply the `inst.spawn` function with a list of addresses under the optional `add_monitors` keyword argument. By default, the actor that spawns a subactor will be added as a monitor together with the `Shard` special actor. If you do not wish to only add the `Shard` actor, you may set `add_monitors` to `False`. If you opt to add extra monitors via the `add_monitors` argument, you will have to also include the spawning actor manually.

If we supply a list of monitors, then the shard will automatically assign the listed actors as monitors and have the sentinel task report to them. We may also
request to monitor a running actor by sending it a request over the message system. The message must follow the format
```python
{"action": "monitor", "address": address_to_report_to}
```
and will receive updates just the same as if it had been added when the actor was spawned.

### Receiving Notifications
When an actor ceases to operate (stops processing messages), the sentinel task will produce a message the contents of which depends on
how this came to be. The message will follow the schema
```python
{
   "action": "sentinel",
   "address": str, # The address of the actor that stopped
   "type": Or(Const("done"), Const("cancellation"), Const("exception")), # The reason for the update
   Optional("exception"): str, # The name of the exception if one occurred
   "tag": str, # A callsign of the sentinel that was triggered. Usually "main_task".
}
```

The user can catch these in a few different ways, but the easiest is to register sentinel handlers.
The actor instance provides two functions for this:

1. `inst.sentinel_handler(name, awt)` which is receives the sentinel message when the address field matches the `name` argument.
2. `@inst.default_sentinel` which receives sentinel messages for which no specific sentinel handler has been registerd.

Alternatively, you can override the default behavior by defining an `@inst.action` for the `"sentinel"` messages.

### Out of the Box Sentinels
The shard actor will always keep a lookout for any actor it has spawned so that it can remove the instance from its internal registries and have it be garbage collected.
The websocket actor will always supervise all `FrontActor`s and `BackActors` to ensure they get removed in a similar fashion.

### What Triggers the Sentinel
There are a few ways to trigger the sentinel. Under the hood, it awaits the message processing task. This means:
1. Any action that cancels the actor, such as invoking the `inst.stop` method, will produce a `"cancellation"` message.
2. Any exception arising inside a message handler will cause an `"exception"` message.
3. Any daemon marked as a `dead_mans_hand` daemon, will cancel the message processing task if it terminates, leading to a `"cancellation"` message
   *even if it was stopped by an exception*.
4. If the websocket consumer task in a `FrontActor` dies, the `WebsocketActor`
   will sense that this has happened. It will directly invoke the stop methods of
   both the actors associated with the connection, and both of them will send
   cancellation messages.

## Miscellaneous Features
### Hashes
In many applications we need some method of keeping track of information like
active session keys across shards. While it is possible to directly use your
favorite in memory database for this purpose, or to implement your own
distributed hash table, the simplest use cases may benefit from Skick's `hash`
system.

In order to create a dictionary which will be eventually consistent across all
actors and shards, simply write

```python
@skick.actor("some_actor")
def the_actor(inst, msg):
   dictionary = skick.hash("table_name")

   ...

   @inst.action("the_action")
   async def the_action(msg):

      ...

      value = await dictionary["our_key"]

      ...
   
   ...
```
These hash objects emulate dictionaries to some extent, but are asynchronous.
They support getting, setting, deleting and checking whether certain keys are
in the table. The supported operations are:

1. Getting, which can be done either with `await hash.get(key)` or `await hash[key]`,
2. Setting, which can be done with `await hash.set(key, value)` or `hash[key] = value`. N.B: Here the former allows you to await completion before proceeding, but the latter simply creates a task that will complete at some unknown time in the future.
3. Deletion, which can be done with `await hash.delete(key)` or `del hash[key]`. As in the setting operation, the former method guarantees not proceeding before the key has been deleted, while the latter only promises to delete the key some time in the future.
4. Checking for membership. Here there is no convenient asynchronous equivalent to the `in` operator, as the `in` operator will convert any future we try to return from the dunder method into a boolean. Therefore only the method `await hash.contains(key)` is available.

These as objects are, as is evident from this description, rather basic. Among
other things, they do not support iteration, can not be copied and can not be
compared with one another. What they do support is both a single core
dictionary based implementation and a clustered Redis based implementation.

In order to use the Redis version, ensure you have a Redis instance running,
and then add the keyword parameter `dict_type` when you instantiate the Skick
instance, setting it to a redis url such as in the following example.

```python
...

skick = Skick(dict_type="redis://user:pwd@localhost:port")

...
```

This may seem like an odd choice of a feature. But it is perfectly in line with
the underlying philosophy of Skick. We do not want the Java experience. We want
minimal boilerplate requirements and we want simple use cases to be available
at the developer's fingertips. If there were no facilities such as the hash
object in Skick, then the developer would have to make his own version of it for
nearly every project. While this would not be altogether difficult, and while it
would allow him to expand its capabilities to encompass more of Redis' (if he
choses Redis) feature set, it would be a major annoyance.

For those applications that require more sophisticated interactions with Redis,
other in memory databases or fully featured SQL databases, the
developer will have to bring his own libraries and implementations.

### Directories
In frameworks like Flask and Sanic, endpoints do not have to be attached directly
to the main application object. This is convenient, because in nontrivial
applications, we want to distribute the code over multiple semi independent modules.

In Skick, there is a similar mechanism called a `Directory`. They act as proxy objects for the main `Skick` object. We can attach actors, sessions and handshakes to a `Directory`, import the `Directory` in the main module, and then
attach them to the main `Skick` object using the `skick.add_directory(directory)` method.

For example, suppose there is a submodule `a.py`. We define an actor there and
use the Directory class to transfer it to the `main.py` file.

```python
#a.py
from skick import Directory

A = Directory()

@A.actor("a_actor")
def a_actor(inst, messages):
   ...
```

We can then use the directory in the file main.py as follows.
```python
#main.py
from skick import Skick
from a import A
...

skick = Skick()
skick.add_directory(A)

...
```
The actor defined in `a.py` is then transferred to the `skick` object in `main.py` and the resulting actor can be used just as if it had been defined directly in `main.py` with the decorator `@skick.actor`.
