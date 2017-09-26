# Spade Edge

Spade Edge is the entry point for data into the Spade pipeline.  It is a
minimally-validating, write-only API server which annotates events and writes
them to Kinesis and S3. The service is typically scaled behind an Elastic Load
Balancer, which handles concerns such as HTTPS. Standard requests result in a 204
No Content, and the persisted event is annotated with source IP, generated UUID,
and server time.

Since Spade Edge is the single entry point from many different servers and many
different domains, it has a configurable cross-domain policy.

## HTTP API

### POST / /track /track/

Data should be sent to Spade with POST; GET is available but deprecated, and will soon have an 8k URI limit.  Spade Edge
accepts any request that is properly formatted with data encoded with standard base64, with padding, using the alphabet<sup>1</sup>:

    ABCDEFGHIJKLMNOPQRSTUVWXYZ
    abcdefghijklmnopqrstuvwxyz
    0123456789+/=

If you are hooking Spade Edge up to the rest of the Spade pipeline, then data (after base64 decoding)
should conform to a JSON encoded object or list of objects that look like the
following (whitespace is not significant) and defined in [Blueprint](https://github.com/twitchscience/blueprint):

    {
        "event": "some-event-to-track",
        "properties": {
            "property1": "value1",
            "otherproperty": "someothervalue"
        }
    }

The two important pieces are `event` and `properties`.  Spade Edge will automatically attach the
current server timestamp, a UUID, the source IP, and optionally the user agent (if `ua=1` is supplied
as a request query parameter) to the raw data provided.

In both cases, the base64 should be posted as urlencoded form style:

    data=eyJldmVudCI6InNvbWUtZXZlbnQtdG8tdHJhY2siLCJwcm9wZXJ0aWVzIjp7Im90aGVycHJvcGVydHkiOiJzb21lb3RoZXJ2YWx1ZSIsInByb3BlcnR5MSI6InZhbHVlMSJ9fQ==

Due to ambiguity in HTTP, the `+` in the base64 alphabet may be decoded to a space by the edge. Both the edge and spade itself will interpret spaces as `+` when base64 decoding to handle this.

Spade Edge will respond with a 204 No Content unless a `img=1` is supplied as a request query parameter, in which
case it will respond with a 200 and a 1x1 transparent pixel.  It will also return a `413` if you send a payload larger than 500 kB.

<sup>1</sup>For compatibility reasons, Spade will also accept the URLSafe Base64 alphabet, but we don't recommend it for new clients.


### GET /healthcheck

Returns a 200 status code without content.

### GET /xarth

Returns a 200 status code with the content `XARTH`.

### GET /crossdomain.xml

Returns an xml document containing the configured cross-domain policy.
